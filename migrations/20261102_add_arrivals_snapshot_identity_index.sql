CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS arrivals_snapshot_identity_unique
    ON public.arrivals (file_id, stop_id, route_id, arrival_time, run_number)
    NULLS NOT DISTINCT;