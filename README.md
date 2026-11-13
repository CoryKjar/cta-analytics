# CTA Analytics

Personal portfolio project collecting CTA 'L' arrival predictions every five minutes into PostgreSQL, with raw XML archiving, recovery, health alerts and a read-only Streamlit dashboard.

## Project Layout

```text
src/
	pipeline.py               One-shot collection (manual or cron fallback)
	worker.py                 Persistent collector (production)
	config/                   Station list
	ingestion/                CTA API requests and raw XML upload
	transform/                XML parsing and source-conflict policy
	storage/                  Database loader, collection ledger, bucket access, held uploads, recovery
	monitoring/               Health checks, email alerts and collection gap report
	utils/                    Logging and staging cleanup
dashboard/                  Read-only operations monitor and analytics page
migrations/                 Additive SQL migrations, applied manually
tests/                      Mocked unit tests
```

## Setup

Use Python 3.12 or newer in an isolated environment:

```powershell
python -m pip install -r requirements.txt
```

Configure `.env` or deployment variables (never commit credentials):

- Collection: `CTA_API_KEY`.
- Database: `DATABASE_URL`.
- Bucket: `BUCKET`, `ENDPOINT`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`; optional `REGION`.
- Email alerts: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_FROM_EMAIL`, `ALERT_TO_EMAIL`.

The project runs against the existing production schema and does not bootstrap a fresh database. No runtime code creates or alters tables. Apply migrations in order:

1. [`20261020_add_collection_attempt_ledger.sql`](migrations/20261020_add_collection_attempt_ledger.sql): one run row and one row per station per collection (about 41,000 rows/day; plan retention). Ledger writes are best-effort and never block collection.
2. [`20261020_add_collection_retry_context.sql`](migrations/20261020_add_collection_retry_context.sql): keeps the last retry's HTTP status and error type. Until applied, the collector logs `collection_retry_context_unavailable reason=migration_required` and the dashboard marks retry context unavailable.
3. [`20261027_add_pending_uploads.sql`](migrations/20261027_add_pending_uploads.sql): holds validated XML when a bucket upload fails. Until applied, those responses are discarded and logged as `event=pending_upload_unavailable`.
4. [`20261102_add_snapshot_capture_timestamp.sql`](migrations/20261102_add_snapshot_capture_timestamp.sql): historical intermediate migration that added fetch-time metadata to `ingestion_files`; the final timestamp model no longer stores this field.
5. [`20261102_add_arrivals_snapshot_identity_index.sql`](migrations/20261102_add_arrivals_snapshot_identity_index.sql): builds the per-file arrivals key concurrently. Run this migration with autocommit enabled; `CREATE INDEX CONCURRENTLY` cannot run inside a transaction.
6. [`20261102_drop_legacy_arrivals_identity.sql`](migrations/20261102_drop_legacy_arrivals_identity.sql): removes the old timestamp-based unique constraint after the updated collector is deployed and verified.
7. [`20261105_add_chicago_timestamps.sql`](migrations/20261105_add_chicago_timestamps.sql): adds the canonical timezone-aware file snapshot time and legacy transition columns/indexes.
8. [`20261105_add_arrivals_inserted_at.sql`](migrations/20261105_add_arrivals_inserted_at.sql): adds a database-default insertion instant for new arrival rows. Historical values remain NULL because the original insert times are unavailable.
9. [`20261106_make_legacy_timestamps_nullable.sql`](migrations/20261106_make_legacy_timestamps_nullable.sql): drops NOT NULL constraints on legacy timestamp columns to support mixed-version deployment during the transition to the new schema. Apply this before deploying the updated collector that uses only `file_generated_timestamp`.
10. [`20261105_drop_redundant_snapshot_timestamps.sql`](migrations/20261105_drop_redundant_snapshot_timestamps.sql): removes duplicate naive snapshot times, stored capture time, and misleading generated/insert fields after every service is deployed with the new model and verified. Never apply while an older deployment is running.

For the DST-safe collector release, apply migrations 4 and 5 first, deploy and verify the updated collector, then apply migration 6. The old collector still needs its timestamp-based constraint until it is replaced. For the simplified timestamp model, apply migrations 7 and 8 first, then apply migration 9, deploy all services and verify collection/recovery/dashboard, then apply migration 10. Never apply migration 10 while an older deployment is running.

Error fields store exception types and safe status codes, never messages, URLs or credentials.

## Entry Points

Run from the project root:

```powershell
python -m src.worker                                 # persistent collector (Railway production)
python -m src.pipeline                               # one collection (manual, or cron fallback)
python -m src.storage.self_heal                      # recovery (Railway cron hourly)
python -m src.monitoring.health_check                # health check and alerts (Railway cron */5)
python -m src.monitoring.collection_gaps --hours 24  # read-only gap report
python -m streamlit run dashboard/app.py             # read-only dashboard
```

Keep `SELF_HEAL_ENABLED=false` on the collector because recovery runs as its own service.

## Collection Lifecycle

For each configured station the collector requests CTA predictions, rejecting API error responses. Transient 429/5xx responses and network timeouts use up to three attempts with bounded backoff; HTTP 409 gets one short retry. It writes the XML locally and uploads it to `raw/YYYY/MM/DD/` in the bucket. Only after uploading does it parse and load predictions in one transaction, register valid empty responses as processed, and clean up staging. Every station's stage and outcome is recorded in `collection_runs` / `collection_attempts`.

The loader is idempotent: it uses the `arrivals` uniqueness constraint, recalculates manifest row counts, and requires the original XML for files with source conflicts or existing audits (`public.arrival_resolution_audit`). Do not replay audited historical files through the regular loader.

## Collection Reliability

A collection interval is a UTC-aligned five-minute window matching cron `*/5`. It is collected when a ledger run started in that window and every station's XML was archived; valid empty responses count. Detection reads only `collection_runs`, `collection_attempts` and `ingestion_files`, so it survives restarts. States from the gap report and health check:

- `missed` / `previous_run_still_active`: Railway skipped the tick because the previous execution was still running.
- `missed` / `not_started`: no run and no processed files. The scheduler did not start the job, or the process failed before reaching the database; platform logs tell which.
- `partial`, `failed`, `incomplete`: some or all stations were not archived, or completion was never recorded.
- `awaiting_recovery`: every station's XML is archived or held, but some files are not yet loaded.
- `unrecorded`: no ledger run, but processed files exist (ledger unavailable).

History before the ledger started (2026-10-20) is not evaluated. During the fall-back DST hour the `unrecorded` cross-check can be off by an hour because source timestamps are naive Chicago times.

Only archived or held XML is recoverable; processed files are never replayed. The CTA Train Tracker API serves only current predictions, so intervals without archived XML remain gaps. They are never zero-filled or reconstructed.

Safeguards:

- A PostgreSQL advisory lock allows one collector at a time across cron, worker and manual runs. A run is also skipped if another run in the same interval is `running` or `completed`; failed or partial runs may retry. Skips log `event=collection_skipped` and exit 0. If the database is unavailable, collection proceeds without these checks.
- `COLLECTION_DEADLINE_SECONDS` (default 240) stops starting new stations so a slow run cannot occupy the next interval. Unstarted stations are recorded `not_attempted` / `DeadlineExceeded`.
- Bucket uploads use 5 s connect / 15 s read timeouts and three standard-mode attempts.
- Collector database connections (ledger, loader, held uploads) use `lock_timeout=30s` and `statement_timeout=120s`, so a stuck lock or query fails the run instead of blocking it.
- If an upload fails after validation, the exact XML bytes (gzip) and SHA-256 are stored in `public.pending_uploads`. The station is recorded at stage `upload_deferred` and not loaded yet. Each recovery run first archives held rows, oldest first, to their original key and deletes a row only after the bucket copy's checksum matches. A different existing object is never overwritten (`ArchiveConflict`). The normal recovery inventory then loads the file. `PENDING_UPLOAD_MAX_ROWS` (default 40,000, about one day; October 26 XML averaged 2.9 KB, about 0.5 KB gzipped) caps the table. If the bucket and database are both unavailable, the response cannot be kept.
- Optional `COLLECTION_HEARTBEAT_URL` (HTTPS only, never logged) is requested after each complete collection. Point it at an external dead-man's-switch service (for example Healthchecks.io or Cronitor; check their current terms) set to alert after about 15 minutes without a ping. The in-project monitor runs on the same platform and was also silent during the 2026-10-21 cron outage.

## Health Checks and Alerts

The monitor raises these issues, logs each at ERROR, emails when the set of issue types changes, and sends a recovery email once checks are clear (SMTP acceptance is not proof of delivery):

- `collection_stale` / `station_collection_stale`: no processed snapshot within `COLLECTION_STALE_MINUTES` (default 15).
- `collection_run_incomplete`: a run has no recorded completion after `COLLECTION_RUN_STALE_MINUTES` (default 15). This does not prove the process is still alive.
- `partial_collection_run`: a run ended partial or failed within the same window.
- `collection_interval_missed`: an interval in the last `COLLECTION_GAP_ALERT_MINUTES` (default 60) has no run.
- `pending_uploads`: held XML older than `PENDING_UPLOAD_ALERT_MINUTES` (default 90) or at the retry cap.
- `failed_files`, `processing_files`, `unexpected_statuses`, `retry_cap`, `row_count_mismatch`: manifest problems.

## Recovery

Recovery archives held uploads, then loads bucket XML that is missing from the manifest or not processed and below `SELF_HEAL_MAX_ATTEMPTS`. At most `SELF_HEAL_MAX_FILES` files are processed per run, in batches of `SELF_HEAL_BATCH_SIZE`. Valid empty files are registered processed. Download, parse and load failures are recorded and retried until the cap. Manifest `row_count` is reconciled from linked arrivals without inventing `processed_at`. Duplicate basenames under different keys stop recovery instead of guessing. `SELF_HEAL_DRY_RUN=true` reports candidates without writes. The bucket listing and database query are not one atomic snapshot, so expect brief discrepancies while collection is active.

## Deployment

The `cta-analytics` collector service runs `python -m src.worker` with no cron schedule and restart policy `ON_FAILURE` (not `ALWAYS`, which would also restart after a clean deploy shutdown). Recovery and the monitor stay on cron. Collector-service variables:

```dotenv
RAILWAY_DEPLOYMENT_OVERLAP_SECONDS=180
RAILWAY_DEPLOYMENT_DRAINING_SECONDS=300
```

On SIGTERM the worker lets the current cycle's collection, load and cleanup finish, then stops; the one-shot pipeline does the same and logs `pipeline_shutdown_completed` or `pipeline_shutdown_incomplete`. SIGKILL and host failures cannot be handled. During deployment overlap, the advisory lock and interval check prevent double collection.

Reconcile abandoned runs only after confirming the old process stopped. Preserve uploaded outcomes, mark the run partial and never fabricate snapshots or finish times.

### Persistent Worker

`python -m src.worker` runs the same pipeline once per UTC interval. It waits on a monotonic clock and realigns to wall-clock boundaries, so run time does not cause drift. It starts immediately if launched within 120 s of an interval start, with no burst catch-up. Cycle failures are logged and the loop continues. A watchdog exits the process with code 1 (`event=worker_cycle_timeout`) if one cycle exceeds `WORKER_CYCLE_TIMEOUT_SECONDS` (default 600), because Railway restarts only processes that exit. On startup it logs persisted gaps from the last `WORKER_STARTUP_GAP_HOURS` (default 6). It stays resident, so memory is billed continuously; locally it used about 119 MB after imports.

To roll back to cron, set the `cta-analytics` schedule to `*/5 * * * *` and the start command to `python -m src.pipeline`. If both ever run, the lock and interval check make the second skip. Switching modes can miss the interval in which the change takes effect (the 2026-10-28 00:30 UTC switch missed one).

## Dashboard

The dashboard shows station coverage, prediction volume, consistency checks, forecast horizons, CTA delay flags and coordinate availability, plus file processing issues and collection run/attempt logs. The sidebar selects a 6-hour, 24-hour, 7-day, 30-day, full-history or custom Chicago date range. It also switches between hourly and daily granularity and filters by station and route. Station filters apply to coverage, prediction and route views; health issues and logs always cover all stations. Daily views sum hourly counts and recompute rates; days cut by the window edge have no coverage value, and wait percentiles are hourly only. Queries use one read-only consistent snapshot and are cached for five minutes. Coverage assumes 12 snapshots per station per full hour, counts valid empty responses, and excludes partial boundary hours. Unknown flags are not treated as false, and Chicago hour labels include the DST offset.

## Logging

All components use a UTC logger: INFO/WARNING to stdout, ERROR to stderr, and a local `logs/pipeline.log` rotating at 5 MB with three backups. Railway container filesystems are not durable. Key events: `pipeline_completed`, `pipeline_incomplete`, `pipeline_failed`, `pipeline_skipped`, `collection_deadline_reached`, `pending_upload_stored`, `arrivals_loaded`, `recovery_completed`, `pending_uploads_drained`, `health_issue`, `health_email_sent`, `worker_cycle_finished`.

## Tests

```powershell
python -B -m unittest discover -s tests -v
```

Tests use mocks for the CTA API, database and bucket. They never touch production data and are not part of the deployed services.

## Data Caveats

- Rows are CTA predictions, not observed arrivals or passenger counts. `wait_time` is a forecast horizon, not a measured wait.
- Source timestamps are naive Chicago wall times; operational timestamps and logs are UTC.
- Account for collection gaps, unknown delay flags and missing coordinates in analysis.
- `ingestion_files.file_generated_timestamp` is the timezone-aware instant parsed from validated CTA `<tmst>`. Existing `created_at` records when the manifest row was inserted.
- `arrivals.prediction_time` and `arrivals.arrival_time` remain CTA source values. `arrivals.inserted_at` is set by PostgreSQL when the arrival row is first inserted; historical values remain NULL because their original insert instants are unknown.
- Repeated fall-back-hour snapshots without an offset-bearing filename token keep `file_generated_timestamp` NULL instead of receiving an invented fold. Such snapshots are excluded from time-bounded dashboard history until their instant can be proved.
- During the staged transition, legacy `collection_timestamp` and capture/generated/insert columns remain in the database. After all services are upgraded, apply `20261105_drop_redundant_snapshot_timestamps.sql`; do not run it during a mixed-version deployment.
- Historical repair tooling and the October 2026 repair record were removed from the working tree. They remain in Git history (`573f224`, `1b43627`); do not rerun old backfills. Production rollback tables and the server-side cutover dump were not touched by this cleanup.
