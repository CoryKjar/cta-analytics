# Historical Production Repair Record

This document records the completed October 17 repair, not current operating instructions. The one-off repair and observer programs have been retired from the runtime checkout; their last deployed versions remain in Git commit `573f224`. Retained database tables, original XML and backups are unchanged. Do not rerun historical backfills on the repaired database.

Authorized scope: align live ingestion/replay/recovery with the audited source policy, finish the staged historical catch-up, deploy compatible code, perform a guarded table cutover, and continue until runtime monitoring gates pass. Retain rollback data; do not delete old tables or raw XML.

## Release Behavior

- Original XML bytes travel with parsed records so concatenation cannot lose per-file evidence.
- The shared loader groups same-source train identities and applies `unknown_conflicts_v1`: disputed headings unknown, disputed GPS pairs unknown, disputed delay flags unknown. Other conflicts fail closed.
- `public.arrival_resolution_audit` retains full compressed XML and policy decisions. Identical replay preserves its audit; changed evidence refuses overwrite. Verified replay retains canonical NULLs instead of choosing source variants.
- Zero-arrival station mapping, correct booleans and train run numbers remain in place.
- Total upload failures, partial collections and parse failures exit nonzero after preserving successful work. Storage diagnostics log safe SDK codes/statuses, never exception URLs or credentials.
- INFO/WARNING use stdout; ERROR/CRITICAL use stderr. Per-station freshness prevents one healthy station hiding missing coverage.
- Health cron changes from daily to every five minutes. Collector stays every five minutes; recovery stays hourly.

## Backup And Cutover

The persistent database volume holds `/var/lib/postgresql/data/cta_repair_backups/before_arrivals_cutover_20261017.dump`, a PostgreSQL custom archive of arrivals, ingestion manifest and monitoring state. The complete archive was decoded with `pg_restore` and checksummed:

```text
71d8d99e378a882f42bab0576e0c477e8ac6baaf984c5786f03dc63dea6034e6
```

The staged replacement is `cta_rebuild_20261016_full`. Full inventory refresh precedes maintenance; recent-day inventory can then refresh the final small delta. It requires a prior full inventory, scans source days covering the previous/current snapshot and pending files, pins downloaded ETags, and must complete after the snapshot is advanced. New/unmanifested or changed sources remain blocking conditions.

During the authorized short window, stop collector/recovery and any local repair writers. Run final catch-up, recent inventory, pending build and full validation. Cutover requires zero live delta/nonprocessed files and source/audit checks, locks/rechecks everything, preserves canonical constraints, copies immutable quality evidence to the live audit table, saves prior manifest metadata and retains the old arrivals table. Resume only compatible code. Existing rollback guards refuse discarding new writes.

## Monitoring Gate

The temporary post-cutover observer followed actual Railway runtime events, not artificial successful test calls. Its required gates were:

1. Three consecutive clean scheduled collector completions with every configured station uploaded and loaded/registered; a gap greater than eight minutes resets the consecutive count.
2. Two clean scheduled health checks after cutover.
3. One clean deployed recovery summary with no load/download/parse failures or retry-cap exhaustion.
4. Independent database checks: manifest counts reconcile, no missing train identities/orphans, recent source timestamps and all stations fresh, and every audited unknown still matches its preserved source and policy.
5. Production dashboard and its read-only database queries work.

Any runtime error or health issue fails the gate; investigate and restart observation after repair. SMTP TLS/authentication was checked separately without sending a test message. Actual inbox delivery is not inferred from successful authentication.

The cutover committed at 2026-10-17 04:54:43 UTC with 3,570,173 predictions across 469,147 manifest files. Validation found zero missing run numbers, manifest count mismatches, processing-history changes or failures among the 1,982 source audits. The observed gate passed with three consecutive clean collector runs, three clean health checks and one clean recovery run. The temporary observer trigger and function were removed afterward.

Historical snapshots absent from the bucket cannot be reconstructed; keep their documented gaps rather than fabricate records. Do not remove rollback tables/backups until separately approved retention expires.