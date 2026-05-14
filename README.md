# watchdog

Generic watchdog runner for unattended long-running backfill jobs.

The package provides:

- `BackfillWatchdogAdapter`: a small protocol for project-specific backfill state.
- `run_watchdog_once`: one watchdog cycle: read state, reset stale work, run one batch, re-read state, format a concise status message.
- `format_watchdog_message`: a six-line human-readable summary suitable for chat notifications.

## Development

```bash
python -m pytest
```

When used from `stock_research`, the legacy `stock_research.backfill_watchdog` module remains as a compatibility shim and forwards to this package.
