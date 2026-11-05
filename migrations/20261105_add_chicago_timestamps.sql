-- Add Chicago-time timestamp columns to ingestion_files and arrivals tables
-- These replace the UTC timestamp columns and remove the need for timezone conversions in queries

ALTER TABLE public.ingestion_files
    ADD COLUMN IF NOT EXISTS file_generated_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS file_insert_timestamp TIMESTAMPTZ;

ALTER TABLE public.arrivals
    ADD COLUMN IF NOT EXISTS row_generated_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS row_insert_timestamp TIMESTAMPTZ;

-- Add indexes on the generated timestamps for query performance
CREATE INDEX IF NOT EXISTS idx_ingestion_files_file_generated_timestamp 
    ON public.ingestion_files (file_generated_timestamp);

CREATE INDEX IF NOT EXISTS idx_arrivals_row_generated_timestamp 
    ON public.arrivals (row_generated_timestamp);
