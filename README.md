# CTA self-healing patch

## Included
- `src/pipeline.py`: current pipeline with an opt-in self-heal hook.
- `src/storage/self_heal.py`: bounded Bucket-to-Postgres recovery worker and derived row-count repair.
- `src/storage/check_reconciliation.py`: read-only integrity report.

## How to apply
Back up the current files, then copy these files into the same relative locations in the CTA project. The pipeline and recovery worker expect your existing `fetch_arrivals`, parser, loader, archive helper, `ingestion_files`, and `arrivals` schema.

## Safe rollout
1. First run a read-only report: `python -m src.storage.check_reconciliation`
2. Dry-run recovery: set `SELF_HEAL_DRY_RUN=true`, then run `python -m src.storage.self_heal`
3. Run a small recovery: set `SELF_HEAL_MAX_FILES=25` and `SELF_HEAL_BATCH_SIZE=25`, then run the worker.
4. For scheduled operation, set `SELF_HEAL_ENABLED=true` only if you intend the regular pipeline invocation to perform the historical Bucket inventory. Otherwise schedule `python -m src.storage.self_heal` daily/weekly externally.
5. Keep `SELF_HEAL_MAX_ATTEMPTS` finite so malformed XML does not retry forever.

## Configuration
Existing `.env` values required: `DATABASE_URL`, `BUCKET`, `ENDPOINT`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`; `REGION` optional.

## Behavior and limitations
- Recovery candidates are Bucket XMLs absent from the manifest or with a non-processed status and attempts below the cap.
- XMLs are downloaded to staging and parsed. Arrival-bearing files go through the existing idempotent loader; valid zero-arrival files are registered processed; parse/download failures are reported and retained for retry/investigation.
- `row_count` is reconciled from actual linked arrivals for processed rows only. It does not fabricate `processed_at`.
- The inventory uses basename because the current manifest schema uses `source_file`. Duplicate basenames under different Bucket keys stop recovery rather than choosing the wrong object.
- A Bucket listing and live database query are not an atomic snapshot. Expect a few transient discrepancies while collection is active.
- This patch does not delete Bucket objects or auto-delete local evidence.
