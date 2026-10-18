import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timezone

import httpx

from nexus_batch.models import CompletionTarget, Config, Entry, Manifest, Request, dumps, fingerprint, jsonl
from nexus_batch.provider import (
    MAX_CONTROL_BYTES,
    FakeProvider,
    Job,
    OpenAIProvider,
    ProviderError,
    reconcile,
)


def manifest(count=3):
    now = datetime.now(timezone.utc)
    entries = []
    for index in range(count):
        request = Request("run", chr(ord("a") + index), f"document {index}")
        entries.append(
            Entry(
                request,
                CompletionTarget("default", str(index), "run", b"token", 1),
                fingerprint(request),
                now,
            )
        )
    return Manifest("run", "batch", Config(), entries, now, 0)


def success_line(request_id, text="text", **body):
    return dumps(
        {
            "custom_id": request_id,
            "response": {
                "status_code": 200,
                "body": {
                    "status": "completed",
                    "output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}],
                    **body,
                },
            },
        }
    ).encode()


class FakeProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_scenarios_correlate_by_id_and_return_one_outcome_per_request(self):
        for scenario, failures, diagnostic in [
            ("success", 0, False),
            ("mixed", 1, False),
            ("expired", 2, False),
            ("missing", 1, True),
            ("duplicate", 1, True),
            ("unknown", 0, True),
            ("malformed", 1, True),
        ]:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                provider = FakeProvider(directory, scenario)
                batch = manifest()
                file_id = await provider.upload(batch.batch_id, jsonl(batch))
                created = await provider.create(file_id, {"batch": batch.batch_id})
                job = await provider.retrieve(created.id)
                self.assertTrue(job.terminal())
                files = list(await provider.results(job))
                outcomes, diagnostics = reconcile(batch, job, files)
                self.assertEqual([outcome.request_id for outcome in outcomes], ["a", "b", "c"])
                self.assertEqual(sum(outcome.failure is not None for outcome in outcomes), failures)
                self.assertEqual(bool(diagnostics), diagnostic)
                for outcome, entry in zip(outcomes, batch.entries):
                    self.assertEqual(outcome.fingerprint, entry.fingerprint)
                    self.assertNotEqual(outcome.result is None, outcome.failure is None)
                if scenario == "success":
                    self.assertEqual(json.loads(files[0].splitlines()[0])["custom_id"], "c")
                    self.assertIn("document 0", outcomes[0].result.text)

    async def test_ambiguous_submission_survives_restart_without_metadata_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = manifest(1)
            provider = FakeProvider(directory, "submission-unknown", processing_delay=0.05)
            file_id = await provider.upload(batch.batch_id, jsonl(batch))
            metadata = {"batch": batch.batch_id, "hash": "original"}
            with self.assertRaises(ProviderError) as failure:
                await provider.create(file_id, metadata)
            self.assertTrue(failure.exception.ambiguous)
            self.assertFalse(failure.exception.permanent)
            metadata["hash"] = "mutated"
            restarted = FakeProvider(directory)
            jobs = await restarted.find({"batch": batch.batch_id, "hash": "original"})
            self.assertEqual(len(jobs), 1)
            self.assertFalse(jobs[0].terminal())
            with self.assertRaises(ProviderError):
                await restarted.results(jobs[0])
            await asyncio.sleep(0.06)
            job = await restarted.retrieve(jobs[0].id)
            outcomes, _ = reconcile(batch, job, list(await restarted.results(job)))
            self.assertIsNotNone(outcomes[0].result)
            self.assertEqual(len(await restarted.find({"batch": batch.batch_id})), 1)

    async def test_hidden_ambiguous_submission_cannot_be_reconciled(self):
        with tempfile.TemporaryDirectory() as directory:
            provider = FakeProvider(directory, "submission-unreconciled")
            batch = manifest(1)
            file_id = await provider.upload(batch.batch_id, jsonl(batch))
            with self.assertRaises(ProviderError):
                await provider.create(file_id, {"batch": batch.batch_id})
            self.assertEqual(await FakeProvider(directory).find({"batch": batch.batch_id}), [])

    async def test_instances_share_all_jobs_and_reject_path_traversal(self):
        with tempfile.TemporaryDirectory() as directory:

            async def submit():
                provider = FakeProvider(directory)
                file_id = await provider.upload("batch", jsonl(manifest(1)))
                return await provider.create(file_id, {"shared": "true"})

            jobs = await asyncio.gather(*(submit() for _ in range(12)))
            provider = FakeProvider(directory)
            self.assertEqual(len(await provider.find({"shared": "true"})), len(jobs))
            with self.assertRaises(ProviderError) as failure:
                await provider.download("../state.json")
            self.assertTrue(failure.exception.permanent)


class ReconcileTests(unittest.TestCase):
    def test_duplicate_oversized_incomplete_and_missing_results(self):
        batch = manifest(5)
        output = b"\n".join(
            [
                success_line("b", "second"),
                success_line("a", "first"),
                success_line("c", "x" * 4096),
                success_line("d", status="incomplete"),
                b"{broken",
                dumps({"custom_id": "unknown", "error": {"code": "test"}}).encode(),
            ]
        )
        errors = dumps({"custom_id": "a", "error": {"code": "batch_expired", "message": "secret"}}).encode()
        outcomes, diagnostics = reconcile(batch, Job("job", "expired"), [output, errors])
        self.assertEqual(
            [item.failure.code if item.failure else "" for item in outcomes],
            ["DUPLICATE_RESULT", "", "RESULT_TOO_LARGE", "INCOMPLETE_RESULT", "BATCH_EXPIRED"],
        )
        self.assertEqual(outcomes[1].result.text, "second")
        self.assertEqual(len(diagnostics), 4)

    def test_terminal_batch_preserves_partial_success(self):
        for status, code in [
            ("failed", "BATCH_FAILED"),
            ("cancelled", "BATCH_CANCELLED"),
            ("expired", "BATCH_EXPIRED"),
            ("completed", "MISSING_RESULT"),
        ]:
            with self.subTest(status=status):
                outcomes, diagnostics = reconcile(manifest(2), Job("job", status), [success_line("a")])
                self.assertIsNotNone(outcomes[0].result)
                self.assertEqual(outcomes[1].failure.code, code)
                self.assertEqual(len(diagnostics), 1)

    def test_safe_bounded_machine_codes_and_diagnostics(self):
        for code, expected in [
            ("rate_limit_exceeded", "rate_limit_exceeded"),
            ("x" * 64, "x" * 64),
            ("x" * 65, ""),
            ("secret\nmessage", ""),
            ("error_é", ""),
        ]:
            data = dumps({"custom_id": "a", "error": {"code": code, "message": "private document"}}).encode()
            outcomes, _ = reconcile(manifest(1), Job("job", "completed"), [data])
            self.assertEqual(outcomes[0].failure.provider_code, expected)
            self.assertNotIn("private document", outcomes[0].failure.message)
        outcomes, diagnostics = reconcile(
            manifest(1),
            Job("job", "secret" * 5000),
            [b"x" * (MAX_CONTROL_BYTES + 1) + b"\n" + b"{bad\n" * 200],
        )
        self.assertEqual(len(diagnostics), 101)
        self.assertEqual(diagnostics[-1], "additional diagnostics omitted")
        self.assertLess(len(outcomes[0].failure.message), 200)

    def test_refusal_and_negative_usage_fail_safely(self):
        refusal = dumps(
            {
                "custom_id": "a",
                "response": {
                    "status_code": 200,
                    "body": {
                        "status": "completed",
                        "output": [
                            {"type": "message", "content": [{"type": "refusal", "refusal": "secret"}]}
                        ],
                    },
                },
            }
        ).encode()
        for data, expected in [
            (refusal, "MODEL_REFUSAL"),
            (success_line("a", usage={"input_tokens": -1}), "INVALID_RESULT"),
        ]:
            outcome = reconcile(manifest(1), Job("job", "completed"), [data])[0][0]
            self.assertEqual(outcome.failure.code, expected)


class OpenAIProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_contract_and_paginated_recovery(self):
        calls = []

        def handle(request):
            calls.append((request.method, request.url.path))
            self.assertEqual(request.headers["authorization"], "Bearer test-secret")
            path = request.url.path
            if request.method == "POST" and path == "/v1/files":
                self.assertIn(b'name="purpose"\r\n\r\nbatch', request.content)
                self.assertIn(b'filename="batch.jsonl"', request.content)
                self.assertIn(b"{input}\n", request.content)
                return httpx.Response(200, json={"id": "file-input"})
            if request.method == "POST" and path == "/v1/batches":
                self.assertEqual(
                    json.loads(request.content),
                    {
                        "input_file_id": "file-input",
                        "endpoint": "/v1/responses",
                        "completion_window": "24h",
                        "metadata": {"batch": "logical"},
                    },
                )
                return httpx.Response(
                    200, json={"id": "job", "status": "validating", "input_file_id": "file-input"}
                )
            if path == "/v1/batches/job":
                return httpx.Response(
                    200,
                    json={
                        "id": "job",
                        "status": "completed",
                        "output_file_id": "output",
                        "error_file_id": "errors",
                    },
                )
            if path == "/v1/batches":
                self.assertEqual(request.url.params["limit"], "100")
                if "after" not in request.url.params:
                    return httpx.Response(
                        200,
                        json={
                            "data": [{"id": "other", "status": "completed"}],
                            "has_more": True,
                            "last_id": "other",
                        },
                    )
                self.assertEqual(request.url.params["after"], "other")
                return httpx.Response(
                    200,
                    json={
                        "data": [{"id": "job", "status": "completed", "metadata": {"batch": "logical"}}],
                        "has_more": False,
                    },
                )
            if path == "/v1/files/output/content":
                return httpx.Response(200, content=b"{output}\n")
            if path == "/v1/files/errors/content":
                return httpx.Response(200, content=b"{errors}\n")
            self.fail(f"unexpected request {request.method} {path}")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            provider = OpenAIProvider("test-secret", client=client)
            file_id = await provider.upload("logical", b"{input}\n")
            created = await provider.create(file_id, {"batch": "logical"})
            job = await provider.retrieve(created.id)
            self.assertEqual(await provider.results(job), (b"{output}\n", b"{errors}\n"))
            self.assertEqual(len(await provider.find({"batch": "logical"})), 1)
        self.assertEqual(calls.count(("POST", "/v1/batches")), 1)
        self.assertEqual(calls.count(("GET", "/v1/batches")), 2)

    async def test_create_never_retries_or_exposes_provider_bodies(self):
        for status, body, permanent, ambiguous in [
            (503, b"test-secret", False, True),
            (429, b"", False, True),
            (400, b"test-secret", True, False),
            (401, b"test-secret", True, False),
            (200, b"{}", False, True),
            (200, b"x" * (MAX_CONTROL_BYTES + 1), False, True),
            (0, b"", False, True),
        ]:
            with self.subTest(status=status, size=len(body)):
                attempts = 0

                def handle(request):
                    nonlocal attempts
                    attempts += 1
                    if status == 0:
                        raise httpx.ConnectError("test-secret", request=request)
                    return httpx.Response(status, content=body)

                async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
                    with self.assertRaises(ProviderError) as failure:
                        await OpenAIProvider("test-secret", client=client).create("input", {})
                self.assertEqual(attempts, 1)
                self.assertEqual(failure.exception.permanent, permanent)
                self.assertEqual(failure.exception.ambiguous, ambiguous)
                self.assertNotIn("test-secret", str(failure.exception))

    async def test_bounded_download_stalled_pagination_and_redirect(self):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"0123456789"))
        ) as client:
            provider = OpenAIProvider("secret", client=client, max_file_bytes=8)
            with self.assertRaises(ProviderError) as failure:
                await provider.download("file")
            self.assertTrue(failure.exception.permanent)
            with self.assertRaises(ProviderError):
                await provider.upload("batch", b"0123456789")
        pages = 0

        def stalled(request):
            nonlocal pages
            pages += 1
            return httpx.Response(
                200,
                json={"data": [{"id": "same", "status": "completed"}], "has_more": True, "last_id": "same"},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(stalled)) as client:
            with self.assertRaises(ProviderError):
                await OpenAIProvider("secret", client=client).find({"batch": "target"})
        self.assertEqual(pages, 2)
        redirects = 0

        def redirect(request):
            nonlocal redirects
            redirects += 1
            return httpx.Response(307, headers={"location": "https://other.example/batches"})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(redirect), follow_redirects=True
        ) as client:
            with self.assertRaises(ProviderError) as failure:
                await OpenAIProvider("secret", client=client).create("file", {})
        self.assertEqual(redirects, 1)
        self.assertTrue(failure.exception.ambiguous)


if __name__ == "__main__":
    unittest.main()
