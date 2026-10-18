import json
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from nexus_batch.models import (
    HARD_MAX_BATCH_BYTES,
    HARD_MAX_BATCH_REQUESTS,
    MAX_REQUEST_BYTES,
    BatchRef,
    CompletionTarget,
    Config,
    Enrollment,
    Entry,
    Failure,
    Manifest,
    Outcome,
    Request,
    Result,
    Usage,
    dumps,
    fingerprint,
    jsonl,
    jsonl_line,
    loads,
    validate_request,
)


class ModelTests(unittest.TestCase):
    def test_store_codec_round_trips_nested_types_and_utc(self):
        now = datetime(2026, 10, 17, 12, 0, tzinfo=timezone(timedelta(hours=-7)))
        request = Request("run", "request", "Summarize café ☕")
        enrollment = Enrollment(
            request,
            CompletionTarget("default", "activity", "attempt-run", b"\x00secret\xff", 2),
            fingerprint(request),
            now,
        )
        encoded = dumps(enrollment)
        decoded = loads(Enrollment, encoded)
        self.assertEqual(enrollment, decoded)
        self.assertEqual(decoded.deadline_at.tzinfo, timezone.utc)
        self.assertIsInstance(decoded.request, Request)
        self.assertIsInstance(decoded.completion_target.task_token, bytes)
        self.assertIn("café", encoded)
        outcome = Outcome(
            "request", fingerprint(request), "batch", result=Result("request", "done", Usage(1, 2))
        )
        self.assertEqual(outcome, loads(Outcome, dumps(outcome)))
        failed = replace(outcome, result=None, failure=Failure("MISSING_RESULT", "missing"))
        self.assertEqual(failed, loads(Outcome, dumps(failed)))
        self.assertEqual("MISSING_RESULT: missing", str(loads(Outcome, dumps(failed)).failure))

    def test_config_rejects_invalid_budgets_and_limits(self):
        Config.defaults("fake").validate()
        with self.assertRaises(ValueError):
            Config.defaults("live").validate()  # The operator chooses the live model.
        replace(Config.defaults("live"), model="test-model").validate()
        invalid = [
            {"window": 0},
            {"poll_interval": float("nan")},
            {"result_deadline": float("inf")},
            {"operation_timeout": 65.0},
            {"max_batch_requests": HARD_MAX_BATCH_REQUESTS + 1},
            {"max_batch_bytes": HARD_MAX_BATCH_BYTES + 1},
            {"max_admissions": -1},
            {"max_output_tokens": 15},
            {"nexus_endpoint": ""},
            {"mode": "other"},
        ]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(Config(), **changes).validate()

    def test_request_size_is_encoded_utf8_size(self):
        request = Request("run", "request", "é" * 100)
        validate_request(request)
        self.assertEqual(fingerprint(request), fingerprint(replace(request)))
        self.assertNotEqual(fingerprint(request), fingerprint(replace(request, input="other")))
        for invalid in [
            replace(request, run_id="../run"),
            replace(request, request_id="x" * 97),
            replace(request, input=""),
            replace(request, input="é" * MAX_REQUEST_BYTES),
        ]:
            with self.subTest(request=invalid.request_id), self.assertRaises(Failure) as failure:
                validate_request(invalid)
            self.assertEqual(failure.exception.code, "INVALID_INPUT")

    def test_jsonl_uses_responses_and_exact_encoded_byte_limit(self):
        config = Config()
        request = Request("run", "request", 'Quote "the document"\nNext line ☕')
        line = jsonl_line(request, config)
        self.assertTrue(line.endswith(b"\n"))
        value = json.loads(line)
        self.assertEqual(value["url"], "/v1/responses")
        self.assertEqual(value["custom_id"], request.request_id)
        self.assertEqual(
            value["body"],
            {"model": config.model, "input": request.input, "max_output_tokens": 128, "store": False},
        )
        now = datetime.now(timezone.utc)
        target = CompletionTarget("default", "activity", "run", b"token", 1)
        manifest = Manifest(
            "run",
            "batch",
            replace(config, max_batch_bytes=len(line)),
            [Entry(request, target, fingerprint(request), now)],
            now,
            len(line),
        )
        self.assertEqual(jsonl(manifest), line)
        with self.assertRaises(ValueError):
            jsonl(replace(manifest, config=replace(config, max_batch_bytes=len(line) - 1)))
        with self.assertRaises(ValueError):
            jsonl(replace(manifest, entries=[]))

    def test_batch_ref_has_only_small_orchestration_fields(self):
        now = datetime.now(timezone.utc)
        reference = BatchRef("run", "batch", Config(), 1000, 4_000_000, now, now, now)
        self.assertLess(len(dumps(reference).encode()), 2048)
        encoded = json.loads(dumps(reference))
        self.assertNotIn("entries", encoded)
        self.assertNotIn("completion_target", encoded)
        self.assertEqual(loads(BatchRef, dumps(reference)), reference)


if __name__ == "__main__":
    unittest.main()
