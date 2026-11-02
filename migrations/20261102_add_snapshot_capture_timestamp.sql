ALTER TABLE public.ingestion_files
    ADD COLUMN IF NOT EXISTS capture_timestamp TIMESTAMPTZ;