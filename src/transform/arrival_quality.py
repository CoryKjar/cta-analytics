"""Canonical source-conflict policy for live ingestion and recovery."""
from collections import defaultdict


COLUMNS = (
    "collection_timestamp", "station_id", "stop_id", "route_id", "destination_id",
    "file_id", "direction", "prediction_time", "arrival_time", "is_approaching",
    "is_delayed", "is_scheduled", "is_fault", "latitude", "longitude", "heading",
    "wait_time", "run_number",
)

POLICY = "unknown_conflicts_v2"
IDENTITY_INDEXES = (0, 2, 3, 8, 17)
ALLOWED_FIELDS = {"heading", "latitude", "longitude", "is_delayed", "is_scheduled", "is_fault"}


def resolve_unknowns(rows):
    """Only agreed values survive; disputed GPS coordinates are unknown as a pair."""
    groups = defaultdict(list)
    for position, row in enumerate(rows, 1):
        if len(row) != len(COLUMNS):
            raise ValueError("Unexpected source record shape.")
        groups[tuple(row[index] for index in IDENTITY_INDEXES)].append((position, row))
    canonical, decisions = [], []
    for identity, variants in groups.items():
        differences = {name for index, name in enumerate(COLUMNS)
                       if any(row[index] != variants[0][1][index] for position, row in variants[1:])}
        if differences - ALLOWED_FIELDS:
            raise ValueError("Source differences exceed the approved staging policy: " + ", ".join(sorted(differences - ALLOWED_FIELDS)))
        null_fields = set(differences)
        if null_fields & {"latitude", "longitude"}:
            null_fields.update(("latitude", "longitude"))
        resolved = list(variants[0][1])
        for name in null_fields:
            resolved[COLUMNS.index(name)] = None
        canonical.append(tuple(resolved))
        if differences:
            decisions.append({
                "identity": dict(zip((COLUMNS[index] for index in IDENTITY_INDEXES), identity)),
                "xml_entries": [position for position, row in variants],
                "different_fields": sorted(differences), "null_fields": sorted(null_fields),
                "reason": "Conflicting source values are unknown; no variant was selected.",
                "normalized_variants": [dict(zip(COLUMNS, row)) for position, row in variants],
            })
    return canonical, decisions