import watchdog_runner


def test_package_exports_backfill_api():
    assert watchdog_runner.BackfillSummary(1, 0, 0, 1, 0, 0, 10).success_tasks == 1
    assert callable(watchdog_runner.run_watchdog_once)
