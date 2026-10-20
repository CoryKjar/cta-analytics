ALTER TABLE public.collection_attempts
    ADD COLUMN IF NOT EXISTS last_retry_http_status INTEGER,
    ADD COLUMN IF NOT EXISTS last_retry_error_type TEXT;