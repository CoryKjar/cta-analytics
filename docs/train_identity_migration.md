# Train Identity Migration

CTA can report two different train runs with the same station, stop, route, prediction time, and arrival time. Run numbers must not be discarded. File 177253 contains runs 812 and 916; the old unique key collapsed 10 API predictions into 9 rows.

## Schema And Compatibility

The parser retains `rn` as validated text in `run_number`. New ingestion requires a nonempty numeric run number and uses:

```text
UNIQUE NULLS NOT DISTINCT
(collection_timestamp, stop_id, route_id, arrival_time, run_number)
```

The migration leaves existing historical run numbers NULL until verified from their source XML. NULL is explicitly an unknown legacy identity, not a fabricated train. Normal loader replay refuses legacy null-run rows; use the source-backed restorer instead. Different run numbers can now have identical arrival times without being collapsed. Conflicting values for the same complete run identity are still rejected.

Do not deploy the new loader before the cutover. The old conflict target stops working after cutover, so collector/recovery jobs must not run old code at that point. Dashboard and read-only consumers can remain online.

## Coordinated Cutover

1. Run tests and back up production according to the operating backup policy.
2. Run `python -m src.storage.migrate_train_identity --phase expand` to inspect without changes. Add `--apply` to add the nullable column and build the unique index concurrently while the old constraint remains active.
3. Stop the old collector and recovery deployments during the maintenance window. Check that no old ingestion job is running.
4. Run `python -m src.storage.migrate_train_identity --phase cutover --apply`. It validates the exact new index and old constraint, then atomically attaches the new constraint and removes the old one with a short lock timeout.
5. Deploy the compatible parser/loader to collector and recovery. Never restart the old image against the cut-over schema.
6. Restore source-verified historical files and verify a fresh collector run before ending maintenance.

Use `--public-url` with Railway-injected `DATABASE_PUBLIC_URL` if the local endpoint is unavailable. Both migration phases default to read-only inspection. Concurrent index creation is a separate autocommit statement; do not wrap expansion in another transaction. An invalid or unexpected index/constraint is a stop condition, not permission to delete arbitrary objects.

Expansion is additive and does not invent historical identities or change prediction timestamps. Cutover does not delete predictions. Rolling back the old unique key is unsafe after distinct same-time trains have been restored, because it would again reject valid data; prefer fixing forward with compatible code.

## Historical Restoration

Preview one file or the unresolved list from the boolean repair:

```powershell
python -m src.storage.restore_train_identity --file-id 177253
python -m src.storage.restore_train_identity --from-checkpoint logs/boolean_repair_checkpoint.json
```

Add `--apply` only after reviewing the source-backed plan. The tool verifies the full XML timestamp, train identities, station, dimensions and immutable stored values. It refuses unmatched rows, inconsistent row counts, and still-conflicting same-run source entries. It locks/rechecks the manifest and prediction rows to prevent overwriting concurrent changes.

Before replacement, it saves compressed before/source/manifest data under `logs/train_identity_backups/`. Each file is restored atomically through a COPY staging table. The restorer replaces that file's rows from the original API, including previously collapsed runs; updates its derived `row_count`; fills a missing station ID if necessary; and preserves status, attempts, errors, `created_at`, `updated_at`, and `processed_at`.

If a file fails, that file rolls back; earlier successfully restored files remain committed. Successful checkpoint-based restoration clears only the resolved entries and only if the checkpoint has not changed concurrently. The checkpoint's database fingerprint must match. No bucket objects are changed.

The original nine unresolved files represent nine confirmed lost predictions. This is not proof that all historical files have had run numbers backfilled. Other pre-migration rows remain explicitly legacy NULL until their own raw sources are restored. Re-run restoration for newly reported ambiguous legacy files; do not pretend an old collapsed row identifies one particular train.

## Verification

```powershell
python -B -m unittest discover -s tests -v
```

Verify the production constraint, first new collector records having non-null runs, distinct 812/916 rows in file 177253, row counts matching each restored manifest, preserved processing timestamps, and successful collector/recovery logs. A migrated schema alone does not prove the compatible collector has deployed.