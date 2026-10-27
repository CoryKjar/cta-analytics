CREATE TABLE IF NOT EXISTS public.pending_uploads (
    source_file TEXT PRIMARY KEY,
    object_key TEXT NOT NULL,
    xml_gzip BYTEA NOT NULL,
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error_type TEXT
);

CREATE INDEX IF NOT EXISTS pending_uploads_created_idx
    ON public.pending_uploads (created_at);
