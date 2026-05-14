from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol


@dataclass(frozen=True)
class BackfillSummary:
    total_tasks: int
    pending_tasks: int
    running_tasks: int
    success_tasks: int
    failed_tasks: int
    skipped_tasks: int
    total_rows_written: int


@dataclass(frozen=True)
class BackfillWatchdogStatus:
    watchdog_action: str
    progress_advanced: bool
    work_remaining: bool
    stale_tasks_reset: int
    timed_out: bool
    previous_frontier: dict[str, str | None]
    current_frontier: dict[str, str | None]


class BackfillWatchdogAdapter(Protocol):
    task_name: str
    dataset: str

    def load_scope(self) -> dict[str, str]: ...
    def load_status_rows(self) -> list[dict[str, Any]]: ...
    def summarize_status(self, rows: list[dict[str, Any]]) -> BackfillSummary: ...
    def compute_frontier(self, rows: list[dict[str, Any]]) -> dict[str, str | None]: ...
    def reset_stale_tasks(self, stale_after_minutes: int) -> int: ...
    def run_once(
        self,
        *,
        scope: dict[str, str],
        max_jobs: int,
        workers: int,
        run_timeout_seconds: int,
    ) -> dict[str, Any]: ...
    def format_extra_status_lines(
        self,
        *,
        rows: list[dict[str, Any]],
        summary: BackfillSummary,
        scope: dict[str, str],
        run_result: dict[str, Any],
        status: BackfillWatchdogStatus,
    ) -> list[str]: ...


def build_watchdog_status(
    *,
    previous_frontier: dict[str, str | None],
    current_frontier: dict[str, str | None],
    pre_summary: BackfillSummary,
    post_summary: BackfillSummary,
    stale_tasks_reset: int,
    timed_out: bool,
    running_jobs_present: bool,
    already_running: bool = False,
) -> BackfillWatchdogStatus:
    frontier_advanced = _frontier_key(current_frontier) > _frontier_key(previous_frontier)
    summary_progress = (
        post_summary.success_tasks > pre_summary.success_tasks
        or post_summary.skipped_tasks > pre_summary.skipped_tasks
        or post_summary.total_rows_written > pre_summary.total_rows_written
    )
    progress_advanced = frontier_advanced or summary_progress
    work_remaining = (
        post_summary.pending_tasks > 0
        or post_summary.running_tasks > 0
        or post_summary.failed_tasks > 0
        or (post_summary.success_tasks + post_summary.skipped_tasks) < post_summary.total_tasks
    )

    if stale_tasks_reset > 0:
        watchdog_action = "restarted"
    elif already_running and (pre_summary.running_tasks > 0 or post_summary.running_tasks > 0):
        watchdog_action = "healthy"
    elif timed_out and running_jobs_present and post_summary.running_tasks > 0:
        watchdog_action = "healthy"
    elif progress_advanced or not work_remaining:
        watchdog_action = "healthy"
    else:
        watchdog_action = "stalled_needs_manual_attention"

    return BackfillWatchdogStatus(
        watchdog_action=watchdog_action,
        progress_advanced=progress_advanced,
        work_remaining=work_remaining,
        stale_tasks_reset=stale_tasks_reset,
        timed_out=timed_out,
        previous_frontier=dict(previous_frontier),
        current_frontier=dict(current_frontier),
    )


def format_watchdog_message(
    *,
    task_name: str,
    dataset: str,
    run_id: str,
    window: str,
    pre_summary: BackfillSummary,
    post_summary: BackfillSummary,
    run_result: dict[str, Any],
    status: BackfillWatchdogStatus,
    extra_lines: list[str],
) -> str:
    completed_through = status.current_frontier.get("completed_through") or "未形成完整前沿"
    currently_working_on = status.current_frontier.get("currently_working_on") or "无"
    run_fields = _parse_extra_fields(extra_lines)
    run_status = str(run_result.get("status") or run_fields.get("run_status") or "unknown")
    run_attempted = _coerce_int(run_result.get("attempted", run_fields.get("run_attempted")))
    run_success = _coerce_int(run_result.get("success", run_fields.get("run_success")))
    run_failed = _coerce_int(run_result.get("failed", run_fields.get("run_failed")))
    run_rows = _coerce_int(run_result.get("rows", run_fields.get("run_rows")))
    title = _task_display_name(task_name)
    lines = [
        f"{title}: {status.watchdog_action}",
        f"进度: 完成至 {completed_through}，当前 {currently_working_on}",
        (
            "任务: "
            f"成功 {post_summary.success_tasks}(+{_non_negative_delta(post_summary.success_tasks, pre_summary.success_tasks)}) / "
            f"待 {post_summary.pending_tasks} / 运行 {post_summary.running_tasks} / "
            f"失败 {post_summary.failed_tasks} / 跳过 {post_summary.skipped_tasks}"
        ),
        (
            "本轮: "
            f"{run_status}，尝试 {run_attempted}，成功 {run_success}，"
            f"失败 {run_failed}，新增行 {run_rows}"
        ),
        (
            f"守护: reset {status.stale_tasks_reset}，timeout {str(status.timed_out).lower()}，"
            f"剩余 {str(status.work_remaining).lower()}"
        ),
        f"范围: {window} | 数据: {dataset}",
    ]
    del run_id
    return "\n".join(lines)


def run_watchdog_once(
    *,
    adapter: BackfillWatchdogAdapter,
    stale_after_minutes: int,
    run_timeout_seconds: int,
    max_jobs: int = 100,
    workers: int = 2,
    send_message: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    scope = adapter.load_scope()
    pre_rows = adapter.load_status_rows()
    pre_summary = adapter.summarize_status(pre_rows)
    previous_frontier = adapter.compute_frontier(pre_rows)
    stale_tasks_reset = adapter.reset_stale_tasks(stale_after_minutes)
    run_result = adapter.run_once(
        scope=scope,
        max_jobs=max_jobs,
        workers=workers,
        run_timeout_seconds=run_timeout_seconds,
    )
    post_rows = adapter.load_status_rows()
    post_summary = adapter.summarize_status(post_rows)
    current_frontier = adapter.compute_frontier(post_rows)
    status = build_watchdog_status(
        previous_frontier=previous_frontier,
        current_frontier=current_frontier,
        pre_summary=pre_summary,
        post_summary=post_summary,
        stale_tasks_reset=stale_tasks_reset,
        timed_out=bool(run_result.get("timed_out")),
        running_jobs_present=post_summary.running_tasks > 0,
        already_running=bool(run_result.get("lock_busy")),
    )
    extra_lines = adapter.format_extra_status_lines(
        rows=post_rows,
        summary=post_summary,
        scope=dict(scope),
        run_result=dict(run_result),
        status=status,
    )
    message = format_watchdog_message(
        task_name=adapter.task_name,
        dataset=adapter.dataset,
        run_id=scope.get("run_id", ""),
        window=scope.get("window", ""),
        pre_summary=pre_summary,
        post_summary=post_summary,
        run_result=run_result,
        status=status,
        extra_lines=extra_lines,
    )
    if send_message is not None:
        send_message(message)

    return {
        "scope": dict(scope),
        "pre_rows": pre_rows,
        "post_rows": post_rows,
        "pre_summary": pre_summary,
        "post_summary": post_summary,
        "previous_frontier": previous_frontier,
        "frontier": current_frontier,
        "status": status,
        "stale_tasks_reset": stale_tasks_reset,
        "run_result": run_result,
        "timed_out": bool(run_result.get("timed_out")),
        "message": message,
    }


def _frontier_key(frontier: dict[str, str | None]) -> tuple[str, str]:
    return (
        frontier.get("completed_through") or "",
        frontier.get("currently_working_on") or "",
    )


def _sanitize_extra_lines(extra_lines: list[str]) -> list[str]:
    sanitized: list[str] = []
    for line in extra_lines:
        normalized = " ".join(str(line).split())
        if not normalized:
            continue
        if "=" not in normalized:
            sanitized.append(f"detail={normalized}")
            continue
        sanitized.append(normalized)
    return sanitized


def _non_negative_delta(current: int, previous: int) -> int:
    return max(0, current - previous)


def _parse_extra_fields(extra_lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in _sanitize_extra_lines(extra_lines):
        key, separator, value = line.partition("=")
        if separator:
            fields[key] = value
    return fields


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _task_display_name(task_name: str) -> str:
    if task_name == "minute_backfill":
        return "分钟线回填 watchdog"
    if task_name == "technical_feature_backfill":
        return "技术指标回填 watchdog"
    return f"{task_name} watchdog"
