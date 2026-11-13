# Running Migrations via Railway CLI (No Local Python Needed)

This is the fastest way to apply the missing database migrations.

## Prerequisites
- Railway CLI installed on your machine
- You're logged into Railway (`railway login`)

## Step 1: Connect to Railway

```powershell
cd C:\Users\Cory\Documents\ml_projects\cta_project
railway link
```

When prompted, select:
- **Project**: `cta-analytics`
- **Environment**: `production` (or your default)

## Step 2: Run the Migration Command

```powershell
railway run python run_migrations.py
```

**Note**: `railway run` executes the command in your Railway environment, which:
- Already has `DATABASE_URL` set
- Already has Python installed
- Has all dependencies (psycopg2, etc.)

## Step 3: Expected Output

You should see:

```
Using DATABASE_URL from environment

Testing connection...
✓ Connected successfully
  PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2)...

============================================================
Running: FRESH_BASELINE_SCHEMA
============================================================
Executing SQL...
✓ FRESH_BASELINE_SCHEMA completed successfully

============================================================
Running: VERIFY_FRESH_SCHEMA
============================================================
Executing SQL...
✓ VERIFY_FRESH_SCHEMA completed successfully

Running: SEED_ROUTES
============================================================
Executing SQL...
✓ SEED_ROUTES completed successfully

Running: 20261020_add_collection_attempt_ledger
============================================================
Executing SQL...
✓ 20261020_add_collection_attempt_ledger completed successfully

Running: 20261020_add_collection_retry_context
============================================================
Executing SQL...
✓ 20261020_add_collection_retry_context completed successfully

[... additional migrations ...]

============================================================
MIGRATION SUMMARY
============================================================
✓ PASS: FRESH_BASELINE_SCHEMA
✓ PASS: VERIFY_FRESH_SCHEMA
✓ PASS: SEED_ROUTES
✓ PASS: 20261020_add_collection_attempt_ledger
✓ PASS: 20261020_add_collection_retry_context
✓ PASS: 20261027_add_pending_uploads
✓ PASS: 20261102_add_arrivals_snapshot_identity_index
✓ PASS: 20261102_add_snapshot_capture_timestamp
✓ PASS: 20261102_drop_legacy_arrivals_identity
✓ PASS: 20261105_add_arrivals_inserted_at
✓ PASS: 20261105_add_chicago_timestamps
✓ PASS: 20261105_drop_redundant_snapshot_timestamps
✓ PASS: 20261106_make_legacy_timestamps_nullable

✓ All migrations completed successfully!

Verifying routes...
✓ Routes table: 8 rows
✓ Destinations table: 0 rows (should be 0)
```

## Step 4: After Migrations Complete

1. **Restart the `cta-health` service** in Railway dashboard:
   - Go to project → `cta-health` service
   - Click the "Restart" button

2. **Verify it's running**: Check logs should show:
   ```
   INFO | __main__ | event=health_check_started
   INFO | __main__ | event=health_check_cycle ...
   ```

3. **If successful**, proceed with Phase 7:
   - Start `cta-recovery` service
   - Start `cta-collector` service

## Troubleshooting

**If you get "railway command not found":**
- Install Railway CLI: `npm install -g @railway/cli`
- Or download from: https://railway.app/docs/cli/cli-installation

**If migrations fail:**
- Report the exact error message
- Check if database connection is working: `railway run psql`

---

That's it! This approach is much faster than setting up local Python. 🚀
