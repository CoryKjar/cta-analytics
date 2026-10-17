"""Expand or cut over train identity; both phases default to read-only inspection."""
import argparse
import os

import psycopg
from dotenv import load_dotenv


INDEX_NAME = "arrivals_run_identity_unique"
IDENTITY_COLUMNS = ["collection_timestamp", "stop_id", "route_id", "arrival_time", "run_number"]


def inspect_run_index(conn):
    return conn.execute("""
        SELECT i.indisvalid, i.indisunique, i.indnullsnotdistinct,
               ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
                     JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
                     ORDER BY k.position)
        FROM pg_index i WHERE i.indexrelid=to_regclass('public.arrivals_run_identity_unique')
    """).fetchone()


def validate_run_index(conn):
    index = inspect_run_index(conn)
    if not index or index[:3] != (True, True, True) or list(index[3]) != IDENTITY_COLUMNS:
        raise RuntimeError("The run-aware unique index is missing, invalid, or has unexpected columns.")


def expand_schema(conn):
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout='3s'")
        conn.execute("ALTER TABLE public.arrivals ADD COLUMN IF NOT EXISTS run_number text")
        column_type = conn.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_schema='public' AND table_name='arrivals' AND column_name='run_number'
        """).fetchone()
        if column_type != ("text",):
            raise RuntimeError("Existing run_number column has an unexpected type.")
    conn.execute("SET statement_timeout='10min'")
    conn.execute("""
        CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS arrivals_run_identity_unique
        ON public.arrivals (collection_timestamp,stop_id,route_id,arrival_time,run_number)
        NULLS NOT DISTINCT
    """)
    validate_run_index(conn)


def cutover_schema(conn):
    """Call only while old collector/recovery deployments are stopped."""
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout='3s'")
        validate_run_index(conn)
        constraints = dict(conn.execute("""
            SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint
            WHERE conrelid='public.arrivals'::regclass AND contype='u'
        """).fetchall())
        old = constraints.get("arrivals_v2_unique")
        if old and old != "UNIQUE (collection_timestamp, stop_id, route_id, arrival_time)":
            raise RuntimeError("Legacy constraint differs from the verified schema; refusing cutover.")
        if INDEX_NAME not in constraints:
            conn.execute("""
                ALTER TABLE public.arrivals ADD CONSTRAINT arrivals_run_identity_unique
                UNIQUE USING INDEX arrivals_run_identity_unique
            """)
        if old:
            conn.execute("ALTER TABLE public.arrivals DROP CONSTRAINT arrivals_v2_unique")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["expand", "cutover"], required=True)
    parser.add_argument("--public-url", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    load_dotenv()
    url = os.getenv("DATABASE_PUBLIC_URL" if args.public_url else "DATABASE_URL")
    if not url:
        parser.error("The selected database URL is not configured.")
    mode = "off" if args.apply else "on"
    with psycopg.connect(url, autocommit=True, connect_timeout=15,
                         options=f"-c default_transaction_read_only={mode} -c statement_timeout=30000") as conn:
        print("Phase:", args.phase, "| Mode:", "APPLY" if args.apply else "READ-ONLY")
        print("Current run-aware index:", inspect_run_index(conn))
        if args.apply:
            (expand_schema if args.phase == "expand" else cutover_schema)(conn)
            print("Phase completed:", args.phase)
        else:
            print("No schema changes applied. Cutover requires old ingestion deployments to be stopped.")


if __name__ == "__main__":
    main()