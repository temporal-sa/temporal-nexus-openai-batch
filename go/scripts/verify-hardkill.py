#!/usr/bin/env python3
"""Test a real worker SIGKILL. Run with server up and other fake workers stopped."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
RUN_ID = "hardkill-" + str(time.time_ns())
STATE = ROOT / ".state" / RUN_ID
STATE.mkdir(parents=True)
BIN = str(ROOT / "bin" / "batchctl")
ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "127.0.0.1:7234")
NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
CONNECTION = ["--address", ADDRESS, "--namespace", NAMESPACE]
EXPECTED_BACKING = "nexus_standalone_activity"
worker = None
demo = None


def start_worker(log, delay):
    return subprocess.Popen(
        [BIN, "worker", *CONNECTION, "--state-dir", str(STATE / "provider"), "--delay", delay],
        stdout=log, stderr=log,
    )


def stop(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def record_inspection(message):
    with (STATE / "inspection.log").open("a") as log:
        log.write(f"{time.monotonic():.3f}: {message}\n")


def wait_worker_ready(process, ready_count, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if process.poll() is not None:
            raise RuntimeError("test worker exited during startup")
        if (STATE / "workers.log").read_text().count("Workers ready:") >= ready_count:
            return
        time.sleep(0.1)
    raise RuntimeError(f"test worker was not ready within {timeout}s")


try:
    with (STATE / "workers.log").open("w") as log:
        worker = start_worker(log, "45s")
        wait_worker_ready(worker, 1)
        demo = subprocess.Popen(
            [BIN, "demo", *CONNECTION, "--count", "3", "--run-id", RUN_ID,
             "--window", "500ms", "--wait", "150s"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        end = time.monotonic() + 35
        before = None
        while time.monotonic() < end:
            if worker.poll() is not None:
                raise RuntimeError("test worker exited before submission; see " + str(STATE / "workers.log"))
            if demo.poll() is not None:
                stdout, stderr = demo.communicate()
                (STATE / "report.json").write_text(stdout)
                (STATE / "demo.stderr.log").write_text(stderr)
                raise RuntimeError("demo exited before an in-progress batch was observed")
            try:
                inspection = subprocess.run(
                    [BIN, "inspect", *CONNECTION, "--run-id", RUN_ID],
                    capture_output=True, text=True, timeout=min(5, max(0.1, end - time.monotonic())),
                )
            except subprocess.TimeoutExpired as error:
                # An inspection can race creation of the PostgreSQL run row.
                # Retry the read while the launcher starts its callers.
                record_inspection(f"transient inspect timeout: {error}")
                continue
            if inspection.returncode == 0:
                try:
                    candidate = json.loads(inspection.stdout)
                except json.JSONDecodeError as error:
                    raise RuntimeError("inspect returned invalid JSON: " + inspection.stdout) from error
                if candidate.get("backing") != EXPECTED_BACKING or candidate.get("storage") != "postgres":
                    raise RuntimeError("hardkill verification requires PostgreSQL-backed Nexus SAA: " + inspection.stdout)
                batches = candidate.get("batches", [])
                if batches and batches[0].get("provider_batch_id") and batches[0].get("provider_status") == "in_progress":
                    before = candidate
                    break
                record_inspection("batch not yet in progress: " + inspection.stdout.strip())
            else:
                record_inspection(f"inspect exited {inspection.returncode}: {inspection.stderr.strip()}")
            time.sleep(0.15)
        if before is None:
            raise RuntimeError("no recorded in-progress provider batch within 35s; ensure other fake workers are stopped")
        (STATE / "before.json").write_text(json.dumps(before, indent=2) + "\n")
        # This is an abrupt process failure, not an SDK graceful Worker.Stop.
        worker.kill()
        worker.wait(timeout=5)
        worker = start_worker(log, "2s")
        wait_worker_ready(worker, 2)
        stdout, stderr = demo.communicate(timeout=110)
        (STATE / "report.json").write_text(stdout)
        (STATE / "demo.stderr.log").write_text(stderr)
        if demo.returncode:
            raise RuntimeError("recovered demo failed: " + stderr.strip())
        report = json.loads(stdout)
        assert report["succeeded"] == 3 and report["failed"] == 0, report
        assert report["status"]["backing"] == EXPECTED_BACKING, report
        old_id = before["batches"][0]["provider_batch_id"]
        assert report["status"]["batches"][0]["provider_batch_id"] == old_id
        state = json.loads((STATE / "provider" / "state.json").read_text())
        assert len(state["jobs"]) == 1, "worker recovery created another provider batch"
        proof = {"run_id": RUN_ID, "signal": "SIGKILL", "succeeded": 3,
                 "backing": report["status"]["backing"],
                 "completion_mode": report["status"]["completion_mode"],
                 "storage": report["status"]["storage"],
                 "provider_jobs": 1, "provider_batch_id": old_id,
                 "elapsed_ns": report["elapsed_ns"], "evidence_directory": str(STATE)}
        (ROOT / ".state" / "hardkill-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
        print(json.dumps(proof, indent=2))
except BaseException:
    print(f"Hardkill verification failed; evidence: {STATE}", file=sys.stderr)
    for name in ("inspection.log", "workers.log", "demo.stderr.log"):
        evidence = STATE / name
        if evidence.exists():
            print(f"--- {name} (tail) ---\n{evidence.read_text()[-6000:]}", file=sys.stderr)
    raise
finally:
    stop(demo)
    stop(worker)
