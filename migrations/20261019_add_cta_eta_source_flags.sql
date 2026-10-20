ALTER TABLE public.arrivals
    ADD COLUMN IF NOT EXISTS is_scheduled BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_fault BOOLEAN;

CREATE TABLE IF NOT EXISTS public.arrival_source_flag_backfill (
    file_id INTEGER PRIMARY KEY REFERENCES public.ingestion_files(file_id),
    policy TEXT NOT NULL,
    source_sha256 CHAR(64) NOT NULL,
    source_row_count INTEGER NOT NULL,
    canonical_row_count INTEGER NOT NULL,
    matched_row_count INTEGER NOT NULL,
    updated_row_count INTEGER NOT NULL,
    unknown_scheduled_rows INTEGER NOT NULL,
    unknown_fault_rows INTEGER NOT NULL,
    resolution_groups INTEGER NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);