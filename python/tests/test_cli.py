import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from nexus_batch import cli


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    monkeypatch.setattr(cli.os, "environ", dict(cli.os.environ))
    for name in (
        "NEXUS_BATCH_MODE",
        "OPENAI_API_KEY",
        "OPENAI_MODEL",
        "TEMPORAL_ADDRESS",
        "TEMPORAL_NAMESPACE",
        "PYTHON_DATABASE_URL",
    ):
        monkeypatch.delenv(name, raising=False)


def test_main_loads_environment_before_parser_defaults(tmp_path, monkeypatch):
    environment = tmp_path / ".env"
    environment.write_text(
        "NEXUS_BATCH_MODE=live\nTEMPORAL_ADDRESS=localhost:1234\n"
        "TEMPORAL_NAMESPACE=test-namespace\nPYTHON_DATABASE_URL=postgres://test/db\n"
    )
    load = cli.load_environment
    monkeypatch.setattr(cli, "load_environment", lambda: load(environment))
    monkeypatch.setattr("sys.argv", ["batchctl-python", "worker"])
    captured = []

    async def run(args):
        captured.append(args)
        return 0

    monkeypatch.setattr(cli, "run", run)
    with pytest.raises(SystemExit) as exit:
        cli.main()
    assert exit.value.code == 0
    assert captured[0].mode == "live"
    assert captured[0].address == "localhost:1234"
    assert captured[0].namespace == "test-namespace"
    assert captured[0].database_url == "postgres://test/db"


def test_exported_environment_wins_over_dotenv(tmp_path, monkeypatch):
    environment = tmp_path / ".env"
    environment.write_text("NEXUS_BATCH_MODE=live\nOPENAI_MODEL=file-model\nOPENAI_API_KEY=file-test-key\n")
    monkeypatch.setenv("NEXUS_BATCH_MODE", "fake")
    monkeypatch.setenv("OPENAI_MODEL", "exported-model")
    monkeypatch.setenv("OPENAI_API_KEY", "exported-test-key")
    cli.load_environment(environment)
    assert cli.parser().parse_args(["worker"]).mode == "fake"
    assert cli.os.environ["OPENAI_MODEL"] == "exported-model"
    assert cli.os.environ["OPENAI_API_KEY"] == "exported-test-key"


def test_mode_defaults_to_fake_and_explicit_mode_overrides_environment(monkeypatch):
    assert cli.parser().parse_args(["worker"]).mode == "fake"
    monkeypatch.setenv("NEXUS_BATCH_MODE", "live")
    assert cli.parser().parse_args(["worker"]).mode == "live"
    args = cli.parser().parse_args(["demo", "--run-id", "test-run", "--mode", "fake"])
    assert args.mode == "fake"
    assert cli.configuration(args).model == "fake-model"


def test_invalid_environment_mode_is_rejected_unless_explicitly_overridden(monkeypatch, capsys):
    monkeypatch.setenv("NEXUS_BATCH_MODE", "invalid")
    with pytest.raises(SystemExit) as exit:
        cli.parser().parse_args(["worker"])
    assert exit.value.code == 2
    assert "NEXUS_BATCH_MODE" in capsys.readouterr().err
    assert cli.parser().parse_args(["worker", "--mode", "fake"]).mode == "fake"


def test_live_model_comes_from_environment_and_cli_wins(monkeypatch):
    monkeypatch.setenv("NEXUS_BATCH_MODE", "live")
    monkeypatch.setenv("OPENAI_MODEL", "test-model")
    args = cli.parser().parse_args(["demo", "--run-id", "test-run"])
    assert cli.configuration(args).model == "test-model"
    explicit = cli.parser().parse_args(["demo", "--run-id", "test-run", "--model", "explicit-test-model"])
    assert cli.configuration(explicit).model == "explicit-test-model"


@pytest.mark.parametrize("key", [None, "", "   "])
async def test_missing_live_key_fails_before_database_or_temporal(monkeypatch, key):
    if key is not None:
        monkeypatch.setenv("OPENAI_API_KEY", key)
    database = AsyncMock()
    temporal = AsyncMock()
    monkeypatch.setattr(cli.Store, "connect", database)
    monkeypatch.setattr(cli.Client, "connect", temporal)
    args = cli.parser().parse_args(["worker", "--mode", "live"])
    with pytest.raises(ValueError, match="OPENAI_API_KEY in .env"):
        await cli.run(args)
    database.assert_not_awaited()
    temporal.assert_not_awaited()


async def test_missing_live_model_gives_configuration_instruction_before_connections(monkeypatch):
    database = AsyncMock()
    temporal = AsyncMock()
    monkeypatch.setattr(cli.Store, "connect", database)
    monkeypatch.setattr(cli.Client, "connect", temporal)
    args = cli.parser().parse_args(["demo", "--mode", "live", "--run-id", "test-run"])
    with pytest.raises(ValueError, match="OPENAI_MODEL in .env or pass --model"):
        await cli.run(args)
    database.assert_not_awaited()
    temporal.assert_not_awaited()


async def test_live_worker_constructs_openai_provider_and_closes_on_shutdown(monkeypatch):
    monkeypatch.setenv("NEXUS_BATCH_MODE", "live")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    store = SimpleNamespace(close=AsyncMock())
    client = object()
    provider = SimpleNamespace(close=AsyncMock())
    openai = Mock(return_value=provider)
    fake = Mock(side_effect=AssertionError("live mode must use OpenAIProvider"))
    monkeypatch.setattr(cli.Store, "connect", AsyncMock(return_value=store))
    monkeypatch.setattr(cli.Client, "connect", AsyncMock(return_value=client))
    monkeypatch.setattr(cli, "OpenAIProvider", openai)
    monkeypatch.setattr(cli, "FakeProvider", fake)
    ready = asyncio.Event()
    observed = []

    @asynccontextmanager
    async def workers(actual_client, actual_store, actual_provider, mode, **options):
        observed.append((actual_client, actual_store, actual_provider, mode))
        ready.set()
        yield

    monkeypatch.setattr(cli, "running_workers", workers)
    args = cli.parser().parse_args(["worker"])
    task = asyncio.create_task(cli.run(args))
    try:
        async with asyncio.timeout(2):
            await ready.wait()
        assert observed == [(client, store, provider, "live")]
        openai.assert_called_once_with("test-key")
        fake.assert_not_called()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    provider.close.assert_awaited_once()
    store.close.assert_awaited_once()
