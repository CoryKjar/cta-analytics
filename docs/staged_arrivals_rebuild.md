# Staged Arrivals Rebuild

The historical rebuild reads original bucket XML into an isolated schema. It does not truncate, delete or replace live arrivals, change source objects, or rewrite live manifest timestamps during initialization, inventory, build, status or validation.

## Current Run

- Schema: `cta_rebuild_20261016_full`.
- Initial manifest cutoff: file ID 648012, containing 464,857 files.
- Initial snapshot time: October 17, 2026 at 02:23:59 UTC (October 16 at 21:23:59 CDT).
- Inventory: 464,857 XML objects, with exact keys and ETags recorded in staging.
- Pilot: 1,000 files, 8,342 distinct predictions, zero errors, no unknown run numbers. Morse runs 812 and 916 remain separate.
- Full build was launched as an independent local process using 32 source readers and 1,000-file transactions. No production cutover was authorized or executed for this run.

Use the project's `cta-env` Python environment and keep the computer awake. The local process needs network access, the Railway CLI session and bucket credentials. It is not a Railway-hosted job and will not run while the computer is shut down. Database progress survives an interruption; there is no endpoint-sensitive local checkpoint.

## Progress

The launched worker writes:

```text
logs/arrivals_rebuild_20261016.stdout.log
logs/arrivals_rebuild_20261016.stderr.log
```

Inspect durable progress, including file counts and staged rows:

```powershell
railway run --service Postgres --environment production --no-local -- python -m src.storage.rebuild_arrivals status --schema cta_rebuild_20261016_full --public-url
Get-Content logs/arrivals_rebuild_20261016.stdout.log -Tail 10
Get-Content logs/arrivals_rebuild_20261016.stderr.log -Tail 20
```

One worker per schema is enforced with a PostgreSQL session advisory lock. `status` is read-only and remains available while a worker holds the lock. Other operations such as inventory, build or validation must wait until that worker stops. Do not run a second writer, boolean repair or manual maintenance during a future cutover window.

The pilot with 250-file transactions ran at approximately 66 files/second. At that rate the initial snapshot takes about two hours; the full job uses larger COPY batches and may differ. This is an estimate, not a completion guarantee. Source errors, database load, network throttling and later catch-up add time.

## Restart Or Retry

If the worker stops, restart the build command. Successfully loaded files are not downloaded again:

```powershell
railway run --service Postgres --environment production --no-local -- python -m src.storage.rebuild_arrivals build --schema cta_rebuild_20261016_full --workers 32 --batch-size 1000 --public-url --apply
```

Each transaction loads a bounded source batch via COPY and advances the file ledger atomically. A crash rolls back its uncommitted batch; already committed batches remain. The staging table is logged/durable, not unlogged. Foreign keys and run-aware uniqueness are enforced while loading. Source reads use `If-Match` against the inventoried ETag, and each loaded file stores a source SHA-256 hash and its independently derived row count.

Valid zero-arrival XMLs produce a loaded ledger record with zero rows and an inferred station ID; they do not create fake arrivals. Exact duplicate source predictions deduplicate only when every stored field agrees for the same complete train identity. Conflicting same-run predictions, missing dimensions, invalid source timestamps or run numbers are recorded as errors, never silently guessed. Different train runs with equal arrival times remain distinct.

Errors stay in the isolated file ledger and block validation/cutover. Inspect them with:

```sql
SELECT file_id, source_file, error
FROM cta_rebuild_20261016_full.files
WHERE status = 'error'
ORDER BY file_id;
```

The first 5,000 files processed by the full worker recorded 14 same-run source conflicts. One inspected example is file 179987 (`20261005_143533_47th_Red.xml`): run 820, stop 30238, arrival 14:43:21 occurs twice in the original XML with the same other fields but headings 212 and 191. The tool does not choose a heading arbitrarily. Counts may increase as more sources are checked; use the ledger for current status. These are not duplicate bucket files, and they are not silently accepted just to finish the rebuild. A source-resolution policy or separately reviewed representation change is required before those files can pass validation.

After resolving the source/configuration issue, rerun `build` with `--retry-errors`. Never mark a failed file loaded manually just to pass validation. Network and parse failures are reported without printing credentials. A database/COPY failure rolls back the batch and stops the worker.

## Read-Only Conflict Classification

The initial pass finished with 462,890 loaded files, 3,523,720 staged predictions and 1,967 same-run source-conflict files. A read-only classifier examined every failed file against its original ETag-pinned XML. It found 1,965 telemetry-only files and two files with contradictory `is_delayed` values; no prediction-time or metadata conflicts. These are file counts, not prediction-group counts. Detailed evidence and the readable report are under `data/conflict_classification_20261017/`.

The classifier preserves every XML variant and compares normalized stored fields; it does not choose a winning record, alter deduplication rules, mark files loaded, or run cutover. To regenerate a report, choose a new output directory:

```powershell
railway run --service Postgres --environment production --no-local -- python -m src.storage.classify_rebuild_conflicts --schema cta_rebuild_20261016_full --workers 32 --public-url --output-dir data/conflict_classification_new
```

The output contains a JSON summary and JSONL evidence with file IDs, source hashes/ETags, identities, XML positions, raw variants and exact differing fields. It uses an enforced read-only database connection and takes the schema advisory lock so the analyzed file ledger cannot be rebuilt concurrently by the normal worker. The classification does not approve a resolution policy; failed files still block validation/cutover.

## Approved Unknown-Value Resolution

After reviewing the classification, the user approved policy `unknown_conflicts_v1` for staging only. Disputed headings become NULL; any conflicting GPS coordinate makes both coordinates NULL; contradictory delayed flags become NULL. All agreed values stay unchanged. Approaching-flag, identity, prediction-time and metadata conflicts are not covered by this policy and still fail closed. No winner, average, true or false value is guessed.

The authorized application resolved all 1,967 files, loaded 19,629 additional canonical predictions, and recorded 2,028 conflict decisions. The original snapshot is now complete: 464,857 loaded files and 3,543,349 staged predictions. Counts of audited unknowns are 1,902 headings, 1,203 GPS pairs and two delayed flags. All source/audit/count checks passed. This did not catch up files after the initial cutoff or swap production.

The dedicated command defaults to read-only planning. It accepts only file IDs from a reviewed classification report and checks exact source filename/key/ETag/hash before planning:

```powershell
railway run --service Postgres --environment production --no-local -- python -m src.storage.resolve_rebuild_conflicts --schema cta_rebuild_20261016_full --classification-dir data/conflict_classification_20261017 --workers 32 --batch-size 100 --public-url
```

Adding `--apply` explicitly permits staging-only writes. `--file-id` can select pilot files. Previously loaded files are not overwritten. Every batch locks and rechecks error-file state, refuses existing staged rows/audits, saves complete original XML and decision evidence to `resolution_audit`, bulk-loads canonical rows and marks those files loaded in one transaction. An interrupted batch rolls back; earlier committed batches remain, and another pass processes only remaining classified error files.

The audit preserves lossless compressed XML, ETag/SHA-256, the policy version, entry positions, normalized variants, changed/nullified fields and reasons. Standard rebuild validation recomputes every audited canonical record from that XML and blocks readiness if the audit or stored values disagree. Full result: `data/conflict_resolution_20261017/report.md`. The earlier classification reports remain immutable evidence of the pre-resolution conflicts.

This policy was not deployed to normal live ingestion or its replay tools. Before production cutover, review how live replay will preserve audited unknowns and how new source conflicts will be handled; do not assume a staging-only rule is automatically a production ingestion policy. Newly reported conflict files require fresh classification evidence. Existing resolution audits remain in the rebuild schema for lookup by file ID and prediction identity after an eventual cutover.

## Validate And Catch Up

Once the worker finishes:

```powershell
railway run --service Postgres --environment production --no-local -- python -m src.storage.rebuild_arrivals validate --schema cta_rebuild_20261016_full --public-url
```

The validator checks pending/error files, missing objects, duplicate basenames, old bucket objects absent from the manifest, source hashes, row-count agreement, train identities, wait-time arithmetic, coordinate ranges, station metadata and pending live collection after the snapshot cutoff. Missing or zero train coordinates supplied by CTA are preserved; `(0,0)` is not fabricated into a station location.

`ready_for_snapshot` is not permission to cut over if newer live files exist. To add those files, advance the snapshot and repeat inventory/build/validation:

```powershell
railway run --service Postgres --environment production --no-local -- python -m src.storage.rebuild_arrivals catch-up --schema cta_rebuild_20261016_full --public-url --apply
railway run --service Postgres --environment production --no-local -- python -m src.storage.rebuild_arrivals inventory --schema cta_rebuild_20261016_full --public-url --apply
railway run --service Postgres --environment production --no-local -- python -m src.storage.rebuild_arrivals build --schema cta_rebuild_20261016_full --workers 32 --batch-size 1000 --public-url --apply
```

Inventory rechecks source identity for loaded files without reloading their rows. Changed object ETags are a validation failure and require investigation. Bucket listings and live collection are not an atomic cross-system snapshot; final inventory and validation belong in the coordinated maintenance window. New/unmanifested objects are not silently discarded or assigned invented manifest history.

## Cutover Requires Separate Approval

The implementation exists, but do not execute it until the complete rebuild is validated and a maintenance window is authorized. Stop collector/recovery deployments, local repair jobs and writable SQL clients first. Perform final catch-up/inventory/build/validation while writers are paused; `pending_live_delta` and nonprocessed files must be zero.

```powershell
railway run --service Postgres --environment production --no-local -- python -m src.storage.rebuild_arrivals cutover --schema cta_rebuild_20261016_full --writers-paused --public-url --apply
```

Under transaction locks, cutover revalidates the snapshot and refuses unsupported dependent views, incoming foreign keys, custom ownership/grants, triggers or row-level security. Such objects require a separately reviewed preservation plan. The swap preserves the canonical run-aware unique constraint expected by ingestion/restoration tooling. It updates only derived live manifest row counts and fills missing station IDs; statuses, attempts, errors and processing timestamps stay intact. A full copy of prior manifest metadata is retained in staging.

The prior arrivals table is retained as `public.cta_rebuild_20261016_full_previous`; no table is dropped. Resume compatible writers only after checking the new table, manifest counts and dashboard consumers. A completed schema is sealed against restarting its staging worker.

## Rollback

There is a guarded rollback command for the maintenance window, before new live writes occur:

```powershell
railway run --service Postgres --environment production --no-local -- python -m src.storage.rebuild_arrivals rollback --schema cta_rebuild_20261016_full --writers-paused --public-url --apply
```

Rollback refuses if new manifest files or changes to processing history have appeared after cutover; reconcile post-cutover data first rather than discard it. It restores the retained live table and manifest row counts/station IDs, keeps the rebuilt table in staging, and deletes neither version. It is not a general undo for arbitrary manual SQL changes. Preserve both tables until rollback retention is explicitly approved to end.

The SQL build/validation/cutover/rollback path was exercised in disposable fixture schemas and verified not to alter the real public tables. Actual production cutover/rollback for this run has not occurred.