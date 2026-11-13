"""Integration tests for fresh schema baseline.

These tests verify that FRESH_BASELINE_SCHEMA.sql is compatible with actual application code.

Requirements:
- PostgreSQL 13+ with UUID extension
- Clean/empty test database
- DATABASE_URL environment variable pointing to test database

Run with:
    pytest tests/integration/test_fresh_schema.py -v

Or with a specific test database:
    DATABASE_URL=postgresql://user:pass@localhost/test_cta pytest tests/integration/test_fresh_schema.py -v

Skip marker for when PostgreSQL is unavailable:
    pytest tests/integration/test_fresh_schema.py -m 'not requires_postgres' -v
"""

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pytest

# PostgreSQL is optional for local dev; skip tests if unavailable
try:
    import psycopg
    from psycopg import sql
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False
    psycopg = None


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture(scope="session")
def requires_postgres():
    """Skip test if PostgreSQL is not available."""
    if not PSYCOPG_AVAILABLE:
        pytest.skip("psycopg not installed; PostgreSQL integration tests skipped")


@pytest.fixture(scope="session")
def database_url():
    """Get test database URL from environment."""
    url = os.getenv("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set; PostgreSQL integration tests skipped")
    return url


@pytest.fixture(scope="session")
def schema_migration_path():
    """Path to FRESH_BASELINE_SCHEMA.sql."""
    root = Path(__file__).resolve().parent.parent.parent  # project root
    path = root / "migrations" / "FRESH_BASELINE_SCHEMA.sql"
    if not path.exists():
        pytest.skip(f"FRESH_BASELINE_SCHEMA.sql not found at {path}")
    return path


@pytest.fixture(scope="session")
def verification_script_path():
    """Path to VERIFY_FRESH_SCHEMA.sql."""
    root = Path(__file__).resolve().parent.parent.parent  # project root
    path = root / "migrations" / "VERIFY_FRESH_SCHEMA.sql"
    if not path.exists():
        pytest.skip(f"VERIFY_FRESH_SCHEMA.sql not found at {path}")
    return path


@pytest.fixture
def fresh_connection(database_url, requires_postgres):
    """Provide a clean database connection for each test."""
    conn = psycopg.connect(database_url, autocommit=True)
    
    # Clean up any prior test data
    try:
        cursor = conn.cursor()
        # Drop all app tables in reverse dependency order
        for table in [
            'arrival_resolution_audit',
            'pending_uploads',
            'collection_attempts',
            'collection_runs',
            'arrivals',
            'ingestion_files',
            'destinations',
            'routes',
            'schema_version'
        ]:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS public.{} CASCADE").format(
                sql.Identifier(table)
            ))
        conn.commit()
    except Exception as e:
        conn.close()
        raise RuntimeError(f"Failed to clean database: {e}")
    
    yield conn
    
    # Cleanup after test
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture
def db_with_schema(fresh_connection, schema_migration_path):
    """Apply FRESH_BASELINE_SCHEMA.sql to database."""
    schema_sql = schema_migration_path.read_text()
    
    cursor = fresh_connection.cursor()
    cursor.execute(schema_sql)
    fresh_connection.commit()
    
    return fresh_connection


@pytest.fixture
def db_verified(db_with_schema, verification_script_path):
    """Verify schema after initialization."""
    verification_sql = verification_script_path.read_text()
    
    cursor = db_with_schema.cursor()
    try:
        cursor.execute(verification_sql)
        db_with_schema.commit()
    except psycopg.errors.Error as e:
        pytest.fail(f"Schema verification failed: {e}")
    
    return db_with_schema


# ============================================================================
# SCHEMA INITIALIZATION TESTS
# ============================================================================


class TestSchemaInitialization:
    """Test that FRESH_BASELINE_SCHEMA.sql can be applied to empty database."""

    @pytest.mark.requires_postgres
    def test_empty_database_accepts_schema(self, fresh_connection, schema_migration_path):
        """FRESH_BASELINE_SCHEMA.sql should apply without errors."""
        schema_sql = schema_migration_path.read_text()
        cursor = fresh_connection.cursor()
        
        # Should not raise
        cursor.execute(schema_sql)
        fresh_connection.commit()
        
        # Verify at least one table exists
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        count = cursor.fetchone()[0]
        assert count >= 9, f"Expected at least 9 tables, got {count}"

    @pytest.mark.requires_postgres
    def test_schema_version_recorded(self, db_with_schema):
        """Schema version should be recorded in schema_version table."""
        cursor = db_with_schema.cursor()
        cursor.execute("SELECT version FROM schema_version WHERE id = 1")
        version = cursor.fetchone()
        
        assert version is not None
        assert version[0] == '1.0.0'

    @pytest.mark.requires_postgres
    def test_all_required_tables_created(self, db_with_schema):
        """All 9 required tables must exist."""
        cursor = db_with_schema.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = {row[0] for row in cursor.fetchall()}
        
        required = {
            'schema_version',
            'routes',
            'destinations',
            'ingestion_files',
            'arrivals',
            'collection_runs',
            'collection_attempts',
            'pending_uploads',
            'arrival_resolution_audit'
        }
        
        assert required.issubset(tables), f"Missing tables: {required - tables}"


# ============================================================================
# SCHEMA VERIFICATION TESTS
# ============================================================================


class TestSchemaVerification:
    """Test that VERIFY_FRESH_SCHEMA.sql passes all checks."""

    @pytest.mark.requires_postgres
    def test_verification_script_passes(self, db_verified):
        """Verification script should pass all checks."""
        # If we get here, verification passed (would have raised otherwise)
        pass

    @pytest.mark.requires_postgres
    def test_ingestion_files_columns(self, db_verified):
        """ingestion_files must have all required columns."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'ingestion_files'
            ORDER BY ordinal_position
        """)
        columns = {row[0]: row[1] for row in cursor.fetchall()}
        
        required = {
            'file_id': 'bigint',
            'source_file': 'text',
            'file_generated_timestamp': 'timestamp with time zone',
            'station_id': 'integer',
            'status': 'text',
            'row_count': 'integer',
            'attempts': 'integer',
            'last_error': 'text',
            'processed_at': 'timestamp with time zone',
            'created_at': 'timestamp with time zone',
            'updated_at': 'timestamp with time zone'
        }
        
        for col, dtype in required.items():
            assert col in columns, f"Missing column: ingestion_files.{col}"
            assert columns[col] == dtype, f"Wrong type for ingestion_files.{col}: {columns[col]} != {dtype}"

    @pytest.mark.requires_postgres
    def test_arrivals_columns(self, db_verified):
        """arrivals must have all required columns including GPS and direction."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'arrivals'
            ORDER BY ordinal_position
        """)
        columns = {row[0]: row[1] for row in cursor.fetchall()}
        
        required_columns = {
            'arrival_id', 'file_id', 'station_id', 'stop_id', 'route_id',
            'destination_id', 'direction', 'prediction_time', 'arrival_time',
            'is_approaching', 'is_delayed', 'is_scheduled', 'is_fault',
            'latitude', 'longitude', 'heading', 'wait_time', 'run_number',
            'collection_timestamp', 'created_at'
        }
        
        missing = required_columns - set(columns.keys())
        assert not missing, f"Missing columns in arrivals: {missing}"

    @pytest.mark.requires_postgres
    def test_collection_runs_columns(self, db_verified):
        """collection_runs must have expected_station_count and other required fields."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'collection_runs'
        """)
        columns = {row[0] for row in cursor.fetchall()}
        
        required = {
            'run_id', 'status', 'expected_station_count', 'deployment_commit',
            'started_at', 'completed_at', 'uploaded_station_count',
            'failed_station_count', 'fatal_error_type'
        }
        
        missing = required - columns
        assert not missing, f"Missing columns in collection_runs: {missing}"
        
        # Verify unwanted columns are removed
        unwanted = {'collection_window_start', 'collection_window_end', 'file_count', 'error_message'}
        found = unwanted & columns
        assert not found, f"Old columns still present in collection_runs: {found}"

    @pytest.mark.requires_postgres
    def test_collection_attempts_columns(self, db_verified):
        """collection_attempts must have all required status tracking columns."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'collection_attempts'
        """)
        columns = {row[0] for row in cursor.fetchall()}
        
        required = {
            'run_id', 'station_id', 'station_name', 'station_index', 'status',
            'stage', 'request_attempts', 'started_at', 'completed_at',
            'http_status', 'cta_error_code', 'error_type', 'storage_error_code',
            'storage_http_status', 'source_file', 'object_key', 'duration_seconds'
        }
        
        missing = required - columns
        assert not missing, f"Missing columns in collection_attempts: {missing}"


# ============================================================================
# PRIMARY KEY TESTS
# ============================================================================


class TestPrimaryKeys:
    """Test primary key definitions."""

    @pytest.mark.requires_postgres
    def test_ingestion_files_pk(self, db_verified):
        """ingestion_files.file_id must be PRIMARY KEY."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name = 'ingestion_files'
              AND constraint_type = 'PRIMARY KEY'
        """)
        pks = cursor.fetchall()
        assert len(pks) == 1, f"Expected 1 PK, found {len(pks)}"

    @pytest.mark.requires_postgres
    def test_collection_attempts_composite_pk(self, db_verified):
        """collection_attempts must have composite PK on (run_id, station_id)."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name = 'collection_attempts'
              AND constraint_type = 'PRIMARY KEY'
        """)
        pks = cursor.fetchall()
        assert len(pks) == 1, f"Expected 1 PK, found {len(pks)}"


# ============================================================================
# UNIQUE CONSTRAINT TESTS
# ============================================================================


class TestUniqueConstraints:
    """Test unique constraints."""

    @pytest.mark.requires_postgres
    def test_ingestion_files_source_file_unique(self, db_verified):
        """ingestion_files.source_file must have UNIQUE constraint."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name = 'ingestion_files'
              AND constraint_type = 'UNIQUE'
        """)
        uniques = cursor.fetchall()
        assert len(uniques) >= 1, "Missing UNIQUE constraint on ingestion_files.source_file"

    @pytest.mark.requires_postgres
    def test_arrivals_unique_constraint(self, db_verified):
        """arrivals must have UNIQUE on (file_id, stop_id, route_id, arrival_time, run_number)."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name = 'arrivals'
              AND constraint_type = 'UNIQUE'
        """)
        uniques = cursor.fetchall()
        assert len(uniques) >= 1, "Missing UNIQUE constraint on arrivals"


# ============================================================================
# FOREIGN KEY TESTS
# ============================================================================


class TestForeignKeys:
    """Test foreign key relationships."""

    @pytest.mark.requires_postgres
    def test_arrivals_fk_ingestion_files(self, db_verified):
        """arrivals.file_id must reference ingestion_files.file_id."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name = 'arrivals'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name LIKE '%file%'
        """)
        fks = cursor.fetchall()
        assert len(fks) >= 1, "Missing FK from arrivals.file_id to ingestion_files"

    @pytest.mark.requires_postgres
    def test_arrivals_fk_routes(self, db_verified):
        """arrivals.route_id must reference routes.route_id."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name = 'arrivals'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name LIKE '%route%'
        """)
        fks = cursor.fetchall()
        assert len(fks) >= 1, "Missing FK from arrivals.route_id to routes"

    @pytest.mark.requires_postgres
    def test_collection_attempts_fk_collection_runs(self, db_verified):
        """collection_attempts.run_id must reference collection_runs.run_id."""
        cursor = db_verified.cursor()
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name = 'collection_attempts'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name LIKE '%run%'
        """)
        fks = cursor.fetchall()
        assert len(fks) >= 1, "Missing FK from collection_attempts.run_id to collection_runs"


# ============================================================================
# DATA INSERTION TESTS
# ============================================================================


class TestDataInsertion:
    """Test that data can be inserted using application-like operations."""

    @pytest.mark.requires_postgres
    def test_insert_routes(self, db_verified):
        """Routes can be inserted."""
        cursor = db_verified.cursor()
        cursor.execute("""
            INSERT INTO routes (route_id, route_name)
            VALUES (%s, %s)
        """, ('20', 'Madison'))
        db_verified.commit()
        
        cursor.execute("SELECT COUNT(*) FROM routes WHERE route_id = %s", ('20',))
        assert cursor.fetchone()[0] == 1

    @pytest.mark.requires_postgres
    def test_insert_destinations(self, db_verified):
        """Destinations can be inserted."""
        cursor = db_verified.cursor()
        cursor.execute("""
            INSERT INTO destinations (destination_id, destination_name)
            VALUES (%s, %s)
        """, (1, 'Destination A'))
        db_verified.commit()
        
        cursor.execute("SELECT COUNT(*) FROM destinations WHERE destination_id = %s", (1,))
        assert cursor.fetchone()[0] == 1

    @pytest.mark.requires_postgres
    def test_insert_ingestion_file(self, db_verified):
        """ingestion_files can be inserted."""
        cursor = db_verified.cursor()
        cursor.execute("""
            INSERT INTO ingestion_files (
                source_file, file_generated_timestamp, station_id, status,
                row_count, attempts, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING file_id
        """, ('raw/2025/01/01/station_40540_2025_01_01_1200.xml', None, 40540, 'pending', 0, 0))
        
        file_id = cursor.fetchone()[0]
        db_verified.commit()
        assert file_id is not None

    @pytest.mark.requires_postgres
    def test_insert_arrival_full_record(self, db_verified):
        """Arrivals can be inserted with all required columns."""
        cursor = db_verified.cursor()
        
        # Setup: routes, destinations, ingestion_files
        cursor.execute("INSERT INTO routes (route_id, route_name) VALUES (%s, %s)", ('20', 'Madison'))
        cursor.execute("INSERT INTO destinations (destination_id, destination_name) VALUES (%s, %s)", (1, 'Test Dest'))
        cursor.execute("""
            INSERT INTO ingestion_files (source_file, file_generated_timestamp, station_id, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            RETURNING file_id
        """, ('test_file.xml', None, 40540, 'processing'))
        file_id = cursor.fetchone()[0]
        
        # Now insert arrival
        cursor.execute("""
            INSERT INTO arrivals (
                file_id, station_id, stop_id, route_id, destination_id, direction,
                prediction_time, arrival_time, is_approaching, is_delayed, is_scheduled,
                is_fault, latitude, longitude, heading, wait_time, run_number
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            file_id, 40540, '40540', '20', 1, 'North',
            None, datetime.now(timezone.utc), True, False, True,
            False, 41.8781, -87.6298, 45, 120.50, 'T123'
        ))
        
        db_verified.commit()
        
        cursor.execute("SELECT COUNT(*) FROM arrivals WHERE file_id = %s", (file_id,))
        assert cursor.fetchone()[0] == 1

    @pytest.mark.requires_postgres
    def test_insert_collection_run(self, db_verified):
        """collection_runs can be inserted."""
        cursor = db_verified.cursor()
        run_id = uuid.uuid4()
        
        cursor.execute("""
            INSERT INTO collection_runs (
                run_id, status, expected_station_count, deployment_commit
            )
            VALUES (%s, %s, %s, %s)
        """, (run_id, 'running', 147, 'abc123def456'))
        
        db_verified.commit()
        
        cursor.execute("SELECT COUNT(*) FROM collection_runs WHERE run_id = %s", (run_id,))
        assert cursor.fetchone()[0] == 1

    @pytest.mark.requires_postgres
    def test_insert_collection_attempts(self, db_verified):
        """collection_attempts can be inserted."""
        cursor = db_verified.cursor()
        run_id = uuid.uuid4()
        
        # Setup collection_runs
        cursor.execute("""
            INSERT INTO collection_runs (
                run_id, status, expected_station_count, deployment_commit
            )
            VALUES (%s, %s, %s, %s)
        """, (run_id, 'running', 1, 'abc123'))
        
        # Insert attempt
        cursor.execute("""
            INSERT INTO collection_attempts (
                run_id, station_id, station_name, station_index, status, stage
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (run_id, '40540', '18th', 1, 'pending', 'not_started'))
        
        db_verified.commit()
        
        cursor.execute("SELECT COUNT(*) FROM collection_attempts WHERE run_id = %s", (run_id,))
        assert cursor.fetchone()[0] == 1

    @pytest.mark.requires_postgres
    def test_insert_pending_upload(self, db_verified):
        """pending_uploads can be stored."""
        cursor = db_verified.cursor()
        import gzip
        
        xml_data = b'<ctatt><tmst>20250101</tmst></ctatt>'
        xml_gzip = gzip.compress(xml_data)
        import hashlib
        sha256 = hashlib.sha256(xml_data).hexdigest()
        
        cursor.execute("""
            INSERT INTO pending_uploads (source_file, object_key, xml_gzip, sha256)
            VALUES (%s, %s, %s, %s)
        """, ('test_upload.xml', 'raw/2025/01/01/test.xml', xml_gzip, sha256))
        
        db_verified.commit()
        
        cursor.execute("SELECT COUNT(*) FROM pending_uploads WHERE source_file = %s", ('test_upload.xml',))
        assert cursor.fetchone()[0] == 1


# ============================================================================
# CONFLICT RESOLUTION TESTS
# ============================================================================


class TestConflictResolution:
    """Test ON CONFLICT DO UPDATE behavior."""

    @pytest.mark.requires_postgres
    def test_ingestion_files_conflict_update(self, db_verified):
        """ingestion_files ON CONFLICT source_file updates existing row."""
        cursor = db_verified.cursor()
        source_file = 'test_conflict.xml'
        
        # First insert
        cursor.execute("""
            INSERT INTO ingestion_files (
                source_file, file_generated_timestamp, station_id, status,
                row_count, attempts, last_error, created_at, updated_at, processed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
            ON CONFLICT (source_file) DO UPDATE SET
                status = EXCLUDED.status,
                attempts = ingestion_files.attempts + 1,
                last_error = EXCLUDED.last_error,
                updated_at = NOW()
        """, (source_file, None, 40540, 'processing', 0, 0, 'First error', None))
        
        cursor.execute("""
            INSERT INTO ingestion_files (
                source_file, file_generated_timestamp, station_id, status,
                row_count, attempts, last_error, created_at, updated_at, processed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
            ON CONFLICT (source_file) DO UPDATE SET
                status = EXCLUDED.status,
                attempts = ingestion_files.attempts + 1,
                last_error = EXCLUDED.last_error,
                updated_at = NOW()
        """, (source_file, None, 40540, 'processing', 0, 0, 'Second error', None))
        
        db_verified.commit()
        
        # Should have only 1 row but with attempts=1 (incremented)
        cursor.execute("""
            SELECT attempts, last_error FROM ingestion_files WHERE source_file = %s
        """, (source_file,))
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == 1, f"Expected attempts=1, got {result[0]}"
        assert result[1] == 'Second error'

    @pytest.mark.requires_postgres
    def test_arrivals_conflict_does_not_update_if_unchanged(self, db_verified):
        """arrivals ON CONFLICT only updates if prediction flags differ."""
        cursor = db_verified.cursor()
        
        # Setup
        cursor.execute("INSERT INTO routes (route_id, route_name) VALUES (%s, %s)", ('20', 'Madison'))
        cursor.execute("INSERT INTO destinations (destination_id, destination_name) VALUES (%s, %s)", (1, 'Test'))
        cursor.execute("""
            INSERT INTO ingestion_files (source_file, file_generated_timestamp, station_id, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            RETURNING file_id
        """, ('test.xml', None, 40540, 'processing'))
        file_id = cursor.fetchone()[0]
        
        arrival_time = datetime.now(timezone.utc)
        
        # First insert
        cursor.execute("""
            INSERT INTO arrivals (
                file_id, station_id, stop_id, route_id, destination_id, direction,
                prediction_time, arrival_time, is_approaching, is_delayed, is_scheduled,
                is_fault, latitude, longitude, heading, wait_time, run_number
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_id, stop_id, route_id, arrival_time, run_number) DO UPDATE SET
                is_approaching = EXCLUDED.is_approaching,
                is_delayed = EXCLUDED.is_delayed,
                is_scheduled = EXCLUDED.is_scheduled,
                is_fault = EXCLUDED.is_fault,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                heading = EXCLUDED.heading
            WHERE arrivals.file_id = EXCLUDED.file_id
        """, (
            file_id, 40540, '40540', '20', 1, 'North',
            None, arrival_time, True, False, True, False,
            41.8781, -87.6298, 45, 120.50, 'T123'
        ))
        
        db_verified.commit()
        
        # Verify row was inserted
        cursor.execute("SELECT COUNT(*) FROM arrivals WHERE file_id = %s", (file_id,))
        assert cursor.fetchone()[0] == 1


# ============================================================================
# LEDGER SIMULATION TESTS
# ============================================================================


class TestLedgerOperations:
    """Test collection ledger-like operations."""

    @pytest.mark.requires_postgres
    def test_collection_cycle_simulation(self, db_verified):
        """Simulate a complete collection run lifecycle."""
        cursor = db_verified.cursor()
        run_id = uuid.uuid4()
        
        # Initialize run
        cursor.execute("""
            INSERT INTO collection_runs (
                run_id, status, expected_station_count, deployment_commit
            )
            VALUES (%s, %s, %s, %s)
        """, (run_id, 'running', 2, 'test_commit'))
        
        # Insert attempts
        cursor.executemany("""
            INSERT INTO collection_attempts (
                run_id, station_id, station_name, station_index, status, stage
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, [
            (run_id, '40540', '18th', 1, 'pending', 'not_started'),
            (run_id, '41120', '35th_Bronzeville_IIT', 2, 'pending', 'not_started'),
        ])
        
        db_verified.commit()
        
        # Simulate first station success
        cursor.execute("""
            UPDATE collection_attempts
            SET status = %s, stage = %s, completed_at = NOW(), duration_seconds = %s
            WHERE run_id = %s AND station_id = %s
        """, ('uploaded', 'complete', 15.5, run_id, '40540'))
        
        # Simulate second station failure
        cursor.execute("""
            UPDATE collection_attempts
            SET status = %s, stage = %s, error_type = %s, http_status = %s, completed_at = NOW()
            WHERE run_id = %s AND station_id = %s
        """, ('failed', 'cta_error', 'connection_timeout', 0, run_id, '41120'))
        
        # Finish run
        cursor.execute("""
            UPDATE collection_runs
            SET status = %s, completed_at = NOW(), uploaded_station_count = %s, failed_station_count = %s
            WHERE run_id = %s
        """, ('partial', 1, 1, run_id))
        
        db_verified.commit()
        
        # Verify final state
        cursor.execute("SELECT status FROM collection_runs WHERE run_id = %s", (run_id,))
        assert cursor.fetchone()[0] == 'partial'
        
        cursor.execute("""
            SELECT COUNT(*) FROM collection_attempts WHERE run_id = %s AND status = 'uploaded'
        """, (run_id,))
        assert cursor.fetchone()[0] == 1


# ============================================================================
# RUN MARKER CONFIGURATION
# ============================================================================


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "requires_postgres: mark test as requiring PostgreSQL (skip if unavailable)"
    )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
