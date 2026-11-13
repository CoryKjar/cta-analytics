#!/usr/bin/env python3
"""
Run database migrations against Railway PostgreSQL.
Requires: psycopg2 (install via: pip install psycopg2-binary)
"""

import os
import sys
from pathlib import Path

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("ERROR: psycopg2 not installed")
    print("Run: pip install psycopg2-binary")
    sys.exit(1)


def get_connection_string():
    """Get DATABASE_URL from environment or prompt user."""
    db_url = os.environ.get("DATABASE_URL")
    
    if db_url:
        print(f"Using DATABASE_URL from environment")
        return db_url
    
    print("DATABASE_URL not set in environment")
    print("Enter full connection string (postgresql://user:password@host:port/dbname):")
    return input("> ").strip()


def read_migration_file(filepath):
    """Read SQL migration file."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Migration file not found: {filepath}")
    
    with open(path, 'r') as f:
        return f.read()


def run_migration(db_url, migration_file, migration_name):
    """Execute a single migration file."""
    print(f"\n{'='*60}")
    print(f"Running: {migration_name}")
    print(f"File: {migration_file}")
    print(f"{'='*60}")
    
    try:
        # Check if this migration needs autocommit mode (CREATE INDEX CONCURRENTLY)
        needs_autocommit = "add_arrivals_snapshot_identity_index" in migration_file
        
        # Connect to database
        conn = psycopg2.connect(db_url)
        
        # Set autocommit if needed
        if needs_autocommit:
            conn.autocommit = True
        
        cursor = conn.cursor()
        
        # Read migration SQL
        sql_content = read_migration_file(migration_file)
        
        # Execute migration
        print(f"Executing SQL...")
        cursor.execute(sql_content)
        
        # Fetch and display results if query returned rows
        if cursor.description:
            print("\nResults:")
            for row in cursor.fetchall():
                print(f"  {row}")
        
        # Commit transaction (if autocommit is off)
        if not needs_autocommit:
            conn.commit()
        
        print(f"✓ {migration_name} completed successfully")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.Error as e:
        print(f"✗ ERROR during {migration_name}:")
        print(f"  {e}")
        return False


def main():
    """Run all migrations in sequence."""
    
    # Get connection string
    db_url = get_connection_string()
    
    if not db_url:
        print("ERROR: No connection string provided")
        sys.exit(1)
    
    # Test connection
    print(f"\nTesting connection...")
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✓ Connected successfully")
        print(f"  PostgreSQL: {version[:50]}...")
        cursor.close()
        conn.close()
    except psycopg2.Error as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)
    
    # Define v1 baseline migration
    migrations = [
        ("SCHEMA_v1", "migrations/SCHEMA_v1.sql"),
    ]
    
    # Run migrations
    results = []
    for name, filepath in migrations:
        success = run_migration(db_url, filepath, name)
        results.append((name, success))
    
    # Summary
    print(f"\n{'='*60}")
    print("MIGRATION SUMMARY")
    print(f"{'='*60}")
    
    for name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(success for _, success in results)
    
    if all_passed:
        print(f"\n✓ All migrations completed successfully!")
        
        # Verify routes
        print(f"\nVerifying routes...")
        try:
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM routes;")
            route_count = cursor.fetchone()[0]
            print(f"✓ Routes table: {route_count} rows")
            
            cursor.execute("SELECT COUNT(*) FROM destinations;")
            dest_count = cursor.fetchone()[0]
            print(f"✓ Destinations table: {dest_count} rows (should be 0)")
            
            cursor.close()
            conn.close()
            
        except psycopg2.Error as e:
            print(f"✗ Verification query failed: {e}")
        
        return 0
    else:
        print(f"\n✗ Some migrations failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
