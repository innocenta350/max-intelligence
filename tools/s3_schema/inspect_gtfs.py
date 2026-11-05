"""S3 GTFS schema inspection tool (M1 Stage S3).

Inspects a staged (extracted) TriMet GTFS feed and writes JSON evidence for
the schema findings documented in ``docs/gtfs-schema-notes.md``. This is the
S3 counterpart of the M0 audit tool: it records what the real feed actually
contains so the S4 parser requirements are evidence-based, not assumed.

Usage (from the repository root, using the project venv):

    .\\.venv\\Scripts\\python.exe tools\\s3_schema\\inspect_gtfs.py <staging-dir> [out.json]

``<staging-dir>`` is an extracted feed directory (an S2 ``staging-*``
directory under ``data/gtfs``). With no ``out.json`` the evidence is written
to ``tools/s3_schema/evidence/gtfs_schema_audit.json``.

The tool reads only; it never modifies the staged feed, touches ``.env``,
or issues network requests. Output contains no credentials.
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from backend.gtfs.csvio import open_gtfs_csv

MAX_ROUTES = {"90", "100", "190", "200", "290"}

REQUIRED_FILES = (
    "feed_info.txt",
    "agency.txt",
    "routes.txt",
    "route_directions.txt",
    "stops.txt",
    "shapes.txt",
    "trips.txt",
    "stop_times.txt",
)

# Columns S4 is expected to require, per file. The tool proves the real
# headers contain them (via open_gtfs_csv's required-column validation).
REQUIRED_COLUMNS = {
    "feed_info.txt": ["feed_version", "feed_start_date", "feed_end_date"],
    "agency.txt": ["agency_id", "agency_name"],
    "routes.txt": ["route_id", "route_type", "route_color", "route_long_name"],
    "route_directions.txt": ["route_id", "direction_id", "direction_name"],
    "stops.txt": [
        "stop_id", "stop_name", "stop_lat", "stop_lon",
        "location_type", "parent_station",
    ],
    "trips.txt": ["route_id", "trip_id", "direction_id", "shape_id"],
    "stop_times.txt": [
        "trip_id", "arrival_time", "departure_time",
        "stop_id", "stop_sequence",
    ],
    "shapes.txt": [
        "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence",
    ],
}


def profile_file(stage: Path, name: str) -> dict:
    """Header, row count, blank counts, and header hygiene for one file."""
    path = stage / name
    info: dict = {"exists": path.exists()}
    if not info["exists"]:
        return info
    info["size_bytes"] = path.stat().st_size
    with open_gtfs_csv(path, required_columns=REQUIRED_COLUMNS[name]) as gtfs:
        info["header"] = list(gtfs.header.columns)
        blanks: Counter = Counter()
        rows = 0
        for _line, record in gtfs.iter_records():
            rows += 1
            for column, value in record.items():
                if value is None or value.strip() == "":
                    blanks[column] += 1
        info["rows"] = rows
        info["blank_counts"] = {k: v for k, v in blanks.items() if v}
        info["row_problems"] = len(gtfs.report.problems)
        info["blank_lines_skipped"] = gtfs.report.blank_lines_skipped
        # Raw (pre-normalisation) header hygiene: unexpected whitespace or
        # duplicate names would be structural surprises for the parser.
        with path.open("rt", encoding="utf-8-sig", newline="") as raw_handle:
            raw = raw_handle.readline().rstrip("\r\n").split(",")
        info["header_names_needing_trim"] = [c for c in raw if c != c.strip()]
        info["duplicate_header_names"] = sorted(
            c for c, n in Counter(c.strip().lower() for c in raw).items() if n > 1
        )
    return info


def inspect(stage: Path) -> dict:
    out: dict = {"files": {}, "route_directions": {}, "routes": {},
                 "stops": {}, "trips_max": {}, "stop_times_max": {},
                 "shapes_max": {}, "csvio_required_columns": {}}

    # route_directions.txt is inspected first on purpose (S3 assignment):
    # its schema drives the direction-name data model.
    for name in REQUIRED_FILES:
        out["files"][name] = profile_file(stage, name)
        try:
            with open_gtfs_csv(stage / name, required_columns=REQUIRED_COLUMNS[name]):
                out["csvio_required_columns"][name] = "ok"
        except Exception as exc:  # noqa: BLE001 - evidence, not control flow
            out["csvio_required_columns"][name] = f"{type(exc).__name__}: {exc}"

    # --- route_directions.txt ------------------------------------------------
    pairs: Counter = Counter()
    names: dict[tuple[str, str], str] = {}
    rd_route_ids: set[str] = set()
    with open_gtfs_csv(stage / "route_directions.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            key = (r.get("route_id") or "", r.get("direction_id") or "")
            pairs[key] += 1
            names[key] = r.get("direction_name") or ""
            rd_route_ids.add(key[0])
    out["route_directions"] = {
        "rows": out["files"]["route_directions.txt"]["rows"],
        "unique_pairs": len(pairs),
        "duplicate_pairs": [list(k) for k, n in pairs.items() if n > 1],
        "direction_id_values": dict(Counter(d for _r, d in pairs)),
        "covers_max_routes": sorted(MAX_ROUTES & rd_route_ids),
        "max_routes_missing": sorted(MAX_ROUTES - rd_route_ids),
        "max_direction_names": {
            rid: {d: names.get((rid, d)) for d in ("0", "1")}
            for rid in sorted(MAX_ROUTES)
        },
    }

    # --- routes.txt -----------------------------------------------------------
    route_ids: set[str] = set()
    type0_rows: list[dict] = []
    with open_gtfs_csv(stage / "routes.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            route_ids.add(r.get("route_id") or "")
            if r.get("route_type") == "0":
                type0_rows.append({
                    "route_id": r.get("route_id"),
                    "agency_id": r.get("agency_id"),
                    "route_short_name": r.get("route_short_name"),
                    "route_long_name": r.get("route_long_name"),
                    "route_color": r.get("route_color"),
                })
    out["routes"] = {
        "route_id_unique": len(route_ids) == out["files"]["routes.txt"]["rows"],
        "route_type0_rows": type0_rows,
        "route_directions_orphan_route_ids": sorted(rd_route_ids - route_ids),
    }

    # --- stops.txt --------------------------------------------------------------
    stop_ids: set[str] = set()
    station_stop_ids: set[str] = set()
    loc_types: Counter = Counter()
    parent_blank = 0
    with open_gtfs_csv(stage / "stops.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            stop_ids.add(r.get("stop_id") or "")
            if r.get("location_type") == "1":
                station_stop_ids.add(r["stop_id"])
            loc_types[r.get("location_type") or "<blank>"] += 1
            if not (r.get("parent_station") or "").strip():
                parent_blank += 1
    out["stops"] = {
        "stop_id_unique": len(stop_ids) == out["files"]["stops.txt"]["rows"],
        "location_type_values": dict(loc_types),
        "rows_with_parent_station": out["files"]["stops.txt"]["rows"] - parent_blank,
        "locid_14024_present": "14024" in stop_ids,
        "locid_14025_present": "14025" in stop_ids,
    }

    # --- trips.txt (MAX scope) ---------------------------------------------------
    max_trip_ids: set[str] = set()
    max_dirs: Counter = Counter()
    max_shape_blank = 0
    per_route_rows: Counter = Counter()
    shapes_by_route: dict[str, set[str]] = defaultdict(set)
    with open_gtfs_csv(stage / "trips.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            if r.get("route_id") in MAX_ROUTES:
                max_trip_ids.add(r.get("trip_id") or "")
                max_dirs[r.get("direction_id") or "<blank>"] += 1
                if not (r.get("shape_id") or "").strip():
                    max_shape_blank += 1
                else:
                    shapes_by_route[r["route_id"]].add(r["shape_id"])
                per_route_rows[r["route_id"]] += 1
    route_list = sorted(shapes_by_route)
    shared_shapes: set[str] = set()
    for index, a in enumerate(route_list):
        for b in route_list[index + 1:]:
            shared_shapes |= shapes_by_route[a] & shapes_by_route[b]
    out["trips_max"] = {
        "trip_count": len(max_trip_ids),
        "rows_per_route": dict(per_route_rows),
        "direction_id_values": dict(max_dirs),
        "shape_id_blank_rows": max_shape_blank,
        "distinct_shape_ids": len(set().union(*shapes_by_route.values())),
        "shapes_shared_across_routes": len(shared_shapes),
        "shapes_per_route": {k: len(v) for k, v in sorted(shapes_by_route.items())},
    }

    # --- stop_times.txt (MAX scope, streamed) -------------------------------------
    max_stop_ids: set[str] = set()
    st_trip_ids: set[str] = set()
    unmatched: Counter = Counter()
    duplicate_seqs = 0
    out_of_order = 0
    st_rows = 0
    seen_seq: set[tuple[str, int]] = set()
    last_seq: dict[str, int] = {}
    station_refs = 0
    arrival_blank = 0
    departure_blank = 0
    with open_gtfs_csv(stage / "stop_times.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            trip_id = r.get("trip_id") or ""
            if trip_id not in max_trip_ids:
                continue
            st_rows += 1
            st_trip_ids.add(trip_id)
            stop_id = r.get("stop_id") or ""
            max_stop_ids.add(stop_id)
            if stop_id in station_stop_ids:
                station_refs += 1
            if stop_id not in stop_ids:
                unmatched[stop_id] += 1
            raw_seq = r.get("stop_sequence") or ""
            try:
                seq = int(raw_seq)
            except ValueError:
                seq = None
            if seq is None:
                duplicate_seqs += 1  # unparseable sequence counts as a defect
            else:
                if (trip_id, seq) in seen_seq:
                    duplicate_seqs += 1
                seen_seq.add((trip_id, seq))
                previous = last_seq.get(trip_id)
                if previous is not None and seq < previous:
                    out_of_order += 1
                last_seq[trip_id] = seq
            if not (r.get("arrival_time") or "").strip():
                arrival_blank += 1
            if not (r.get("departure_time") or "").strip():
                departure_blank += 1
    out["stop_times_max"] = {
        "rows": st_rows,
        "distinct_trips": len(st_trip_ids),
        "max_trips_without_stop_times": len(max_trip_ids - st_trip_ids),
        "distinct_stop_ids": len(max_stop_ids),
        "unmatched_stop_ids": dict(unmatched.most_common(20)),
        "unmatched_distinct": len(unmatched),
        "duplicate_or_unparseable_stop_sequence": duplicate_seqs,
        "out_of_order_rows": out_of_order,
        "arrival_time_blank": arrival_blank,
        "departure_time_blank": departure_blank,
    }

    # --- shapes.txt (MAX scope) -----------------------------------------------------
    max_shape_ids: set[str] = set().union(*shapes_by_route.values())
    points: Counter = Counter()
    bad_coords = 0
    seen_shape_seqs: set[tuple[str, int]] = set()
    shape_seq_dupes = 0
    with open_gtfs_csv(stage / "shapes.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            shape_id = r.get("shape_id") or ""
            if shape_id not in max_shape_ids:
                continue
            points[shape_id] += 1
            try:
                float(r.get("shape_pt_lat") or "")
                float(r.get("shape_pt_lon") or "")
            except ValueError:
                bad_coords += 1
            raw_seq = r.get("shape_pt_sequence") or ""
            try:
                seq_key = (shape_id, int(raw_seq))
            except ValueError:
                seq_key = None
            if seq_key is None:
                shape_seq_dupes += 1
            elif seq_key in seen_shape_seqs:
                shape_seq_dupes += 1
            else:
                seen_shape_seqs.add(seq_key)
    out["shapes_max"] = {
        "referenced_shape_ids": len(max_shape_ids),
        "missing_from_shapes_txt": sorted(max_shape_ids - set(points)),
        "points_total": sum(points.values()),
        "points_per_shape_min": min(points.values()) if points else None,
        "points_per_shape_max": max(points.values()) if points else None,
        "non_numeric_coords": bad_coords,
        "duplicate_or_unparseable_sequences": shape_seq_dupes,
    }

    # --- MAX stop/station structure (parent_station join) ----------------------------
    max_stops: dict[str, dict] = {}
    with open_gtfs_csv(stage / "stops.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            if r.get("stop_id") in max_stop_ids:
                max_stops[r["stop_id"]] = r
    parents = Counter(
        r["parent_station"] for r in max_stops.values()
        if (r.get("parent_station") or "").strip()
    )
    platforms_with_parent = sum(
        1 for r in max_stops.values() if (r.get("parent_station") or "").strip()
    )
    station_ids = {
        r["stop_id"] for r in max_stops.values() if r.get("location_type") == "1"
    }
    out["max_stops"] = {
        "distinct_stop_ids": len(max_stops),
        "location_type_values": dict(
            Counter(r.get("location_type") or "<blank>" for r in max_stops.values())
        ),
        "platforms_with_parent_station": platforms_with_parent,
        "distinct_parent_stations": len(parents),
        "parent_targets_missing_from_stops": sorted(p for p in parents if p not in stop_ids),
        "max_stops_without_parent_station": len(max_stops) - platforms_with_parent,
        "stop_times_rows_referencing_station_rows": station_refs,
        "lat_lon_blank": sum(
            1 for r in max_stops.values()
            if not r["stop_lat"].strip() or not r["stop_lon"].strip()
        ),
        "station_rows_among_max_stops": len(station_ids),
    }
    return out


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    stage = Path(sys.argv[1])
    if not stage.is_dir():
        print(f"staging directory not found: {stage}", file=sys.stderr)
        return 2
    destination = (
        Path(sys.argv[2]) if len(sys.argv) > 2
        else Path(__file__).resolve().parent / "evidence" / "gtfs_schema_audit.json"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    evidence = inspect(stage)
    destination.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(f"evidence written: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
