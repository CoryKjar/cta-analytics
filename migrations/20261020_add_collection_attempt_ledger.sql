CREATE TABLE IF NOT EXISTS public.collection_runs (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'partial', 'failed')),
    expected_station_count INTEGER NOT NULL CHECK (expected_station_count >= 0),
    uploaded_station_count INTEGER NOT NULL DEFAULT 0 CHECK (uploaded_station_count >= 0),
    failed_station_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_station_count >= 0),
    deployment_commit TEXT,
    fatal_error_type TEXT
);

CREATE INDEX IF NOT EXISTS collection_runs_status_started_idx
    ON public.collection_runs (status, started_at DESC);

CREATE TABLE IF NOT EXISTS public.collection_attempts (
    run_id UUID NOT NULL REFERENCES public.collection_runs(run_id) ON DELETE CASCADE,
    station_id INTEGER NOT NULL,
    station_name TEXT NOT NULL,
    station_index INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'uploaded', 'failed', 'not_attempted')),
    stage TEXT NOT NULL,
    request_attempts SMALLINT NOT NULL DEFAULT 0 CHECK (request_attempts >= 0),
    http_status INTEGER,
    cta_error_code TEXT,
    storage_error_code TEXT,
    storage_http_status INTEGER,
    error_type TEXT,
    source_file TEXT,
    object_key TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    PRIMARY KEY (run_id, station_id),
    UNIQUE (run_id, station_index)
);

CREATE INDEX IF NOT EXISTS collection_attempts_run_status_idx
    ON public.collection_attempts (run_id, status);