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

## Tests

```powershell
python -B -m unittest discover -s tests -v
```

Tests use mocked database/bucket clients and cover active parsing, train identity, loading, source conflicts, audit protection, health checks and logging. They do not execute live collection or recovery.

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
