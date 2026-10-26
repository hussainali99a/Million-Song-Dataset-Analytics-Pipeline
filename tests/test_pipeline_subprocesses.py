from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import pytest

import run_pipeline


@pytest.fixture
def logger() -> logging.Logger:
    test_logger = logging.getLogger("test_pipeline_subprocesses")
    test_logger.handlers.clear()
    test_logger.addHandler(logging.NullHandler())
    test_logger.propagate = False
    return test_logger


def python_command(script: str) -> list[str]:
    return [sys.executable, "-c", script]


def test_run_step_returns_successful_child_output(logger: logging.Logger) -> None:
    result = run_pipeline.run_step(
        logger,
        "test success",
        python_command("print('child completed')"),
        timeout=5,
    )

    assert result["status"] == "success"
    assert result["exit_code"] == 0
    assert result["timeout_seconds"] == 5
    assert result["_output_lines"] == ["child completed"]


def test_run_step_raises_with_output_for_nonzero_exit(logger: logging.Logger) -> None:
    with pytest.raises(subprocess.CalledProcessError) as error:
        run_pipeline.run_step(
            logger,
            "test failure",
            python_command("import sys; print('diagnostic'); sys.exit(7)"),
            timeout=5,
        )

    assert error.value.returncode == 7
    assert error.value.output_lines == ["diagnostic"]
    assert error.value.output == "diagnostic\n"


def test_subprocess_log_redacts_common_credential_values() -> None:
    line = "password is hunter2 token=abc123 Authorization: Bearer xyz789"

    redacted = run_pipeline.redact_subprocess_output(line)

    assert "hunter2" not in redacted
    assert "abc123" not in redacted
    assert "xyz789" not in redacted
    assert "[REDACTED]" in redacted


def test_run_step_terminates_child_when_timeout_expires(
    logger: logging.Logger,
) -> None:
    started = time.monotonic()

    with pytest.raises(run_pipeline.StageTimeoutError) as error:
        run_pipeline.run_step(
            logger,
            "slow stage",
            python_command("import time; print('started', flush=True); time.sleep(30)"),
            timeout=0.5,
        )

    assert error.value.stage_name == "slow stage"
    assert error.value.timeout_seconds == 0.5
    assert error.value.return_code is not None
    assert error.value.termination in {"terminated", "killed"}
    assert error.value.output_lines == ["started"]
    assert time.monotonic() - started < 5


def test_run_step_cleans_up_child_when_interrupted(
    monkeypatch: pytest.MonkeyPatch,
    logger: logging.Logger,
) -> None:
    real_popen = subprocess.Popen
    child_process: subprocess.Popen[str] | None = None

    class InterruptingProcess:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            nonlocal child_process
            child_process = real_popen(*args, **kwargs)
            self.interrupted = False

        def __enter__(self) -> Self:
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> bool | None:
            assert child_process is not None
            return child_process.__exit__(exc_type, exc_value, traceback)

        @property
        def pid(self) -> int:
            assert child_process is not None
            return child_process.pid

        @property
        def returncode(self) -> int | None:
            assert child_process is not None
            return child_process.returncode

        def poll(self) -> int | None:
            assert child_process is not None
            return child_process.poll()

        def terminate(self) -> None:
            assert child_process is not None
            child_process.terminate()

        def kill(self) -> None:
            assert child_process is not None
            child_process.kill()

        def wait(self, timeout: float | None = None) -> int:
            assert child_process is not None
            return child_process.wait(timeout=timeout)

        def communicate(self, timeout: float | None = None) -> tuple[str, None]:
            if not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt
            assert child_process is not None
            stdout, stderr = child_process.communicate(timeout=timeout)
            return stdout, stderr

    monkeypatch.setattr(run_pipeline.subprocess, "Popen", InterruptingProcess)
    with pytest.raises(KeyboardInterrupt):
        run_pipeline.run_step(
            logger,
            "interrupted stage",
            python_command("import time; time.sleep(30)"),
            timeout=10,
        )

    assert child_process is not None
    assert child_process.returncode is not None


def test_terminate_process_kills_child_that_ignores_terminate(
    logger: logging.Logger,
) -> None:
    class UnresponsiveProcess:
        pid = 42
        returncode: int | None = None

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            pass

        def wait(self, timeout: float | None = None) -> int:
            if timeout is not None and self.returncode is None:
                raise subprocess.TimeoutExpired(["child"], timeout)
            self.returncode = -9
            return self.returncode

        def kill(self) -> None:
            self.returncode = -9

    child = UnresponsiveProcess()
    assert run_pipeline.terminate_process(child, logger, "unresponsive stage") == "killed"
    assert child.returncode == -9


def test_stage_timeout_supports_global_and_stage_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(run_pipeline.PIPELINE_TIMEOUT_ENV, "20")
    assert run_pipeline.stage_timeout_seconds("Build dbt models") == 20

    monkeypatch.setenv("MUSIC_PIPELINE_TIMEOUT_BUILD_DBT_MODELS_SECONDS", "12.5")
    assert run_pipeline.stage_timeout_seconds("Build dbt models") == 12.5
    assert run_pipeline.stage_timeout_seconds("Run dbt data-quality tests") == 20


@pytest.mark.parametrize("value", ["0", "-2", "nan", "inf", "not-a-number"])
def test_stage_timeout_rejects_invalid_environment_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv(run_pipeline.PIPELINE_TIMEOUT_ENV, value)

    with pytest.raises(ValueError, match="Invalid timeout configuration"):
        run_pipeline.stage_timeout_seconds("Extract HDF5 to Parquet")


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), True])
def test_run_step_rejects_invalid_timeout_values(
    logger: logging.Logger,
    value: float,
) -> None:
    error_type = TypeError if isinstance(value, bool) else ValueError
    with pytest.raises(error_type, match="positive finite"):
        run_pipeline.run_step(
            logger,
            "invalid timeout",
            python_command("pass"),
            timeout=value,
        )


@pytest.mark.parametrize("failure", ["nonzero", "timeout", "interrupt"])
def test_main_does_not_run_downstream_stages_after_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    log_path = tmp_path / "pipeline.log"
    report_dir = tmp_path / "reports"
    dbt_project = tmp_path / "music_analytics"
    dbt_project.mkdir()
    (dbt_project / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")
    dbt_executable = tmp_path / "dbt.exe"
    dbt_executable.touch()

    test_logger = logging.getLogger(f"pipeline_main_{failure}")
    test_logger.handlers.clear()
    test_logger.addHandler(logging.NullHandler())
    test_logger.propagate = False

    monkeypatch.setattr(run_pipeline, "LOG_DIR", tmp_path)
    monkeypatch.setattr(run_pipeline, "REPORT_DIR", report_dir)
    monkeypatch.setattr(run_pipeline, "DBT", dbt_executable)
    monkeypatch.setattr(run_pipeline, "DBT_PROJECT", dbt_project)
    monkeypatch.setattr(run_pipeline, "DBT_PROFILES", dbt_project)
    monkeypatch.setattr(
        run_pipeline,
        "configure_logging",
        lambda _run_id: (test_logger, log_path),
    )
    monkeypatch.setattr(
        run_pipeline,
        "get_stages",
        lambda: [
            ("first prerequisite", ["first"]),
            ("downstream stage", ["second"]),
        ],
    )
    monkeypatch.setattr(run_pipeline, "stage_timeout_seconds", lambda _name: 1.0)

    calls: list[str] = []

    def fail_first_stage(
        _logger: logging.Logger,
        name: str,
        _command: list[str],
        *,
        timeout: float,
    ) -> dict[str, Any]:
        calls.append(name)
        assert timeout == 1.0
        if failure == "timeout":
            raise run_pipeline.StageTimeoutError(
                name,
                timeout,
                process_id=123,
                return_code=-1,
                termination="terminated",
                output_lines=[],
            )
        if failure == "interrupt":
            raise KeyboardInterrupt
        raise subprocess.CalledProcessError(2, ["first"])

    monkeypatch.setattr(run_pipeline, "run_step", fail_first_stage)

    expected_return_code = 130 if failure == "interrupt" else 1
    assert run_pipeline.main() == expected_return_code
    assert calls == ["first prerequisite"]
    report_file = next(report_dir.glob("pipeline_*.json"))
    report = json.loads(report_file.read_text(encoding="utf-8"))
    assert report["status"] == ("cancelled" if failure == "interrupt" else "failed")
    assert [stage["status"] for stage in report["stages"]] == [
        "timed_out"
        if failure == "timeout"
        else "cancelled"
        if failure == "interrupt"
        else "failed"
    ]
