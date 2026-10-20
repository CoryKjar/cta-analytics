# CTA Analytics

Personal portfolio project collecting CTA arrival predictions into PostgreSQL, with recovery, health checks and a read-only dashboard.

## Project Layout

```text
dashboard/                  Read-only Streamlit monitoring dashboard
src/
	pipeline.py               Scheduled collector entry point
	ingestion/                CTA API collection and raw XML upload
	transform/                XML parsing and source-conflict policy
	storage/                  Live database loader, bucket access, audit writes and recovery
	monitoring/               Scheduled health checks and email alerts
tests/                      Mocked regression tests
docs/                       Historical production repair record
data/                       Local data and generated reports
logs/                       Local logs, checkpoints and retained backups
```

The parser also verifies original XML for audited replays. The small source-conflict policy module prevents arbitrary values being chosen when predictions disagree; the loader preserves the evidence in `public.arrival_resolution_audit`.

## Setup

Use Python 3.12 or newer and install the project dependencies in an isolated environment:

```powershell
python -m pip install -r requirements.txt
```

Configure `.env` or deployment variables:

- Collection: `CTA_API_KEY`.
- Database: `DATABASE_URL`.
- Bucket: `BUCKET`, `ENDPOINT`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`; optional `REGION`.
- Email alerts: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_FROM_EMAIL`, `ALERT_TO_EMAIL`.

The existing PostgreSQL schema, including its audit and monitoring tables, is required. This repository runs against that database; it does not bootstrap a fresh schema. Never commit credentials.

The arrivals schema requires nullable `is_scheduled` and `is_fault` columns. Production schema migration and the backfill from retained XML are complete; per-file hashes and match/update counts are recorded in `public.arrival_source_flag_backfill`. Existing v1 source-resolution audit rows remain immutable and were not rewritten. The updated collector is deployed and stores both flags for new arrivals. Do not replay audited historical files through the regular loader to populate the new fields.

## Entry Points

Run commands from the project root using the configured project environment:

```powershell
python -m src.pipeline
python -m src.storage.self_heal
python -m src.monitoring.health_check
python -m streamlit run dashboard/app.py
```

The scheduled collector, recovery and monitor entry points are unchanged. Collection and recovery write bucket/database data; the health command may send alerts and writes monitoring state. The dashboard is read-only.

Production schedules are collection and health checks every five minutes, with recovery hourly. Keep `SELF_HEAL_ENABLED=false` for ordinary collection when recovery is scheduled separately.

The dashboard includes a 24-hour, 7-day, 30-day or full-history monitor for station coverage, prediction volume, consistency checks, forecast horizons, CTA-reported delay flags and coordinate availability. Historical queries use one read-only consistent database snapshot and cache results for five minutes; Refresh clears the cache. Empty station responses count toward coverage. Nominal coverage assumes 12 snapshots per configured station per full hour; partial boundary hours are excluded from hourly coverage. Missing hours remain visible, unknown delay flags are not treated as false, and Chicago hour labels distinguish DST offsets. Forecast horizons and reported flags describe CTA's prediction feed; without observed arrival outcomes, the plots do not measure prediction accuracy.

## Tests

```powershell
python -B -m unittest discover -s tests -v
```

Tests use mocked database/bucket clients and cover active parsing, train identity, loading, source conflicts, audit protection, health checks and logging. They do not execute live collection or recovery.

## Logging

Collection, loading, local cleanup, recovery and scheduled health checks use the same UTC timestamped logger. INFO/WARNING events go to stdout and ERROR events go to stderr, so Railway can distinguish normal activity from failures. Local output also goes to `logs/pipeline.log`.

The local file rotates at 5 MB and retains three backups (`pipeline.log.1` through `.3`), approximately 20 MB total. Components share a file handler within a process; rotation is not coordinated between separate processes writing the same local path. Railway services have separate container filesystems, and local files are not durable unless storage is mounted. Railway console-log retention is separate.

Filter service logs using these event names:

- `event=file_parse_failed` or `event=pipeline_failed`: collector failures.
- `event=arrivals_loaded`: committed prediction rows and file count.
- `event=recovery_file_failed`: filename, failed stage and safe error type.
- `event=recovery_file_completed`: successfully loaded or empty snapshot.
- `event=recovery_completed`: recovery counts, failures, repaired row counts and retry-cap skips; ERROR if failures or exhausted retries remain.
- `event=health_check_completed` or `event=health_issue`: health summary and detected issue types.
- `event=health_email_sent`: alert or recovery email accepted by the configured SMTP transport; not proof of inbox delivery.

Dry runs emit `event=recovery_dry_run_completed` without writes. Operational failures log exception types rather than raw exception messages, signed URLs, connection strings or tracebacks. The manifest still stores the latest file outcome, not a complete attempt history. Logging does not change recovery selection, retry policy, health thresholds or email deduplication.

## Recovery

Start with `SELF_HEAL_DRY_RUN=true` to inspect candidates without writes. Before a write-enabled pilot, set it to `false`, set `SELF_HEAL_MAX_FILES=25` and `SELF_HEAL_BATCH_SIZE=25`, then run the recovery worker. Keep `SELF_HEAL_MAX_ATTEMPTS` finite so invalid files do not retry forever.

## Behavior and limitations
- Recovery candidates are Bucket XMLs absent from the manifest or with a non-processed status and attempts below the cap.
- XMLs are downloaded to staging and parsed. Arrival-bearing files go through the existing idempotent loader; valid zero-arrival files are registered processed; parse/download failures are reported and retained for retry/investigation.
- `row_count` is reconciled from actual linked arrivals for processed rows only. It does not fabricate `processed_at`.
- The inventory uses basename because the current manifest schema uses `source_file`. Duplicate basenames under different Bucket keys stop recovery rather than choosing the wrong object.
- A Bucket listing and live database query are not an atomic snapshot. Expect a few transient discrepancies while collection is active.
- Source timestamps are Chicago-local CTA wall times; processing timestamps and logs are UTC instants.
- Rows are predictions, not confirmed arrivals or passenger counts. `wait_time` is a forecast horizon, not measured waiting time. Account for historical collection gaps, unknown flags and missing coordinates in analytics.

## Historical Repairs

Completed migration/rebuild commands, duplicate manual replay tools and one-off verification scripts have been removed from the runtime project. Their last deployed implementations remain in Git commit `573f224`; do not rerun old backfills against the repaired database.

The [production repair record](docs/production_repair_20261017.md), raw XML, database rollback tables, local backups and audit evidence are retained. This cleanup does not delete production data or alter deployment schedules.
