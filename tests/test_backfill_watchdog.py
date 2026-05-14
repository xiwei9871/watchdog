from watchdog_runner.backfill import (
    BackfillSummary,
    BackfillWatchdogStatus,
    build_watchdog_status,
    format_watchdog_message,
    run_watchdog_once,
)


def test_build_watchdog_status_is_restarted_when_stale_tasks_reset():
    status = build_watchdog_status(
        previous_frontier={"completed_through": "2024-03", "currently_working_on": "2024-04"},
        current_frontier={"completed_through": "2024-03", "currently_working_on": "2024-04"},
        pre_summary=BackfillSummary(20, 10, 1, 9, 0, 0, 10_000),
        post_summary=BackfillSummary(20, 11, 0, 9, 0, 0, 10_000),
        stale_tasks_reset=2,
        timed_out=False,
        running_jobs_present=False,
    )

    assert status.watchdog_action == "restarted"
    assert status.progress_advanced is False
    assert status.work_remaining is True


def test_build_watchdog_status_treats_lock_busy_with_running_tasks_as_healthy():
    status = build_watchdog_status(
        previous_frontier={"completed_through": "2024-03", "currently_working_on": "2024-04"},
        current_frontier={"completed_through": "2024-03", "currently_working_on": "2024-04"},
        pre_summary=BackfillSummary(20, 2, 1, 17, 0, 0, 100_000),
        post_summary=BackfillSummary(20, 2, 1, 17, 0, 0, 100_000),
        stale_tasks_reset=0,
        timed_out=False,
        running_jobs_present=True,
        already_running=True,
    )

    assert status.watchdog_action == "healthy"
    assert status.progress_advanced is False
    assert status.work_remaining is True


def test_format_watchdog_message_is_compact_and_readable():
    status = BackfillWatchdogStatus(
        watchdog_action="healthy",
        progress_advanced=True,
        work_remaining=True,
        stale_tasks_reset=0,
        timed_out=False,
        previous_frontier={"completed_through": "2024-03", "currently_working_on": "2024-04"},
        current_frontier={"completed_through": "2024-04", "currently_working_on": "2024-05"},
    )

    message = format_watchdog_message(
        task_name="minute_backfill",
        dataset="market.stock_minute_bar",
        run_id="minute-backfill:5min:raw,qfq:2024-01-01:2026-05-13",
        window="2024-01-01..2026-05-13",
        pre_summary=BackfillSummary(100, 20, 0, 70, 1, 9, 1_000_000),
        post_summary=BackfillSummary(100, 10, 1, 80, 1, 9, 1_120_000),
        run_result={"status": "completed", "attempted": 10, "success": 10, "failed": 0, "rows": 120_000},
        status=status,
        extra_lines=["run_status=completed", "run_attempted=10", "run_success=10"],
    )

    lines = message.splitlines()
    assert len(lines) == 6
    assert lines[0] == "分钟线回填 watchdog: healthy"
    assert "完成至 2024-04，当前 2024-05" in message
    assert "成功 80(+10)" in message
    assert "新增行 120000" in message
    assert "run_id=" not in message


class FakeAdapter:
    task_name = "minute_backfill"
    dataset = "market.stock_minute_bar"

    def __init__(self) -> None:
        self.scope = {"run_id": "minute-bars", "window": "2024-01..2024-04"}
        self.pre_summary = BackfillSummary(2, 1, 1, 0, 0, 0, 100)
        self.post_summary = BackfillSummary(2, 0, 0, 1, 0, 0, 240)
        self.previous_frontier = {"completed_through": None, "currently_working_on": "2024-04"}
        self.current_frontier = {"completed_through": "2024-04", "currently_working_on": None}
        self.calls: list[str] = []

    def load_scope(self) -> dict[str, str]:
        self.calls.append("load_scope")
        return dict(self.scope)

    def load_status_rows(self) -> list[dict[str, str]]:
        self.calls.append("load_status_rows")
        return [{"status": "running"}] if self.calls.count("load_status_rows") == 1 else [{"status": "success"}]

    def summarize_status(self, rows: list[dict[str, str]]) -> BackfillSummary:
        self.calls.append("summarize_status")
        return self.pre_summary if rows[0]["status"] == "running" else self.post_summary

    def compute_frontier(self, rows: list[dict[str, str]]) -> dict[str, str | None]:
        self.calls.append("compute_frontier")
        return dict(self.previous_frontier if rows[0]["status"] == "running" else self.current_frontier)

    def reset_stale_tasks(self, stale_after_minutes: int) -> int:
        self.calls.append("reset_stale_tasks")
        return 1

    def run_once(
        self,
        *,
        scope: dict[str, str],
        max_jobs: int,
        workers: int,
        run_timeout_seconds: int,
    ) -> dict[str, object]:
        self.calls.append("run_once")
        return {"attempted": 1, "success": 1, "failed": 0, "rows": 140, "status": "completed", "timed_out": False}

    def format_extra_status_lines(self, **kwargs: object) -> list[str]:
        self.calls.append("format_extra_status_lines")
        return []


def test_run_watchdog_once_calls_adapter_and_returns_message():
    adapter = FakeAdapter()
    sent: list[str] = []

    result = run_watchdog_once(
        adapter=adapter,
        stale_after_minutes=20,
        run_timeout_seconds=900,
        send_message=sent.append,
    )

    assert adapter.calls == [
        "load_scope",
        "load_status_rows",
        "summarize_status",
        "compute_frontier",
        "reset_stale_tasks",
        "run_once",
        "load_status_rows",
        "summarize_status",
        "compute_frontier",
        "format_extra_status_lines",
    ]
    assert result["status"].watchdog_action == "restarted"
    assert result["frontier"] == {"completed_through": "2024-04", "currently_working_on": None}
    assert sent == [result["message"]]
