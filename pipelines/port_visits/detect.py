"""Pure, conservative visit detection. No database or network dependencies."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import re


GRID_SIZE_DEG = 0.1


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode()).hexdigest()


def timestamp(value):
    # Python 3.9 accepts microseconds, while AIS source timestamps have nanoseconds.
    value = re.sub(r'(\.\d{6})\d+', r'\1', value)
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)  # ClickHouse query explicitly uses UTC.
    return result.astimezone(timezone.utc)


def distance_m(lat, lon, port):
    lat1, lat2 = math.radians(lat), math.radians(port['latitude'])
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(math.radians(port['longitude']-lon)/2)**2
    return 6371008.8 * 2 * math.asin(math.sqrt(min(1, max(0, a))))


def validate_ports(ports):
    ids = set()
    if not ports:
        raise ValueError('Port dataset is empty')
    for port in ports:
        if not port['port_id'] or port['port_id'] in ids or not port['name']:
            raise ValueError('Port IDs must be nonempty and unique; names are required')
        ids.add(port['port_id'])
        for field, low, high in (('latitude', -90, 90), ('longitude', -180, 180), ('radius_m', 1, 50000)):
            if not math.isfinite(port[field]) or not low <= port[field] <= high:
                raise ValueError('Invalid port ' + field)


def grid_cell(latitude, longitude):
    """Return deterministic spatial-grid coordinates for one point."""
    return (
        math.floor(latitude / GRID_SIZE_DEG),
        math.floor(longitude / GRID_SIZE_DEG),
    )


def build_port_grid(ports):
    """Index ports by geographic grid cell."""
    grid = {}

    for port in ports:
        cell = grid_cell(port['latitude'], port['longitude'])
        grid.setdefault(cell, []).append(port)

    # Stable ordering keeps detection deterministic.
    for cell in grid:
        grid[cell] = sorted(grid[cell], key=lambda port: port['port_id'])

    return grid


def nearby_ports(latitude, longitude, grid, max_radius_m):
    """Return ports from grid cells that could contain a port within max_radius_m."""
    lat_cell, lon_cell = grid_cell(latitude, longitude)

    # Conservative metres-per-degree estimates.
    # Using 100 km rather than ~111 km intentionally searches a slightly
    # wider region so the prefilter cannot exclude a valid nearby port.
    lat_m_per_degree = 100_000.0

    cos_lat = abs(math.cos(math.radians(latitude)))
    lon_m_per_degree = max(100_000.0 * cos_lat, 1_000.0)

    lat_span = math.ceil(max_radius_m / (lat_m_per_degree * GRID_SIZE_DEG)) + 1
    lon_span = math.ceil(max_radius_m / (lon_m_per_degree * GRID_SIZE_DEG)) + 1

    candidates = []

    for lat_offset in range(-lat_span, lat_span + 1):
        for lon_offset in range(-lon_span, lon_span + 1):
            candidates.extend(
                grid.get(
                    (lat_cell + lat_offset, lon_cell + lon_offset),
                    (),
                )
            )

    return candidates


def detect(rows, ports, min_stay=1200, max_gap=900, max_speed=3.0):
    """Require a continuous sequence of low-speed observations inside a port circle.

    A gap or fast/unknown-speed observation breaks a stay. Departure is only
    populated when a timely position is geographically outside the previous port.
    Input conflicts at the same MMSI/time are discarded rather than arbitrarily chosen.
    """
    validate_ports(ports)
    if not all(math.isfinite(v) for v in (min_stay, max_gap, max_speed)) or min_stay <= 0 or max_gap <= 0 or max_speed < 0:
        raise ValueError('Invalid detection thresholds')
    radii = {p['port_id']: p['radius_m'] for p in ports}
    port_grid = build_port_grid(ports)
    max_port_radius = max(radii.values())
    stats = Counter(source_rows=len(rows))
    groups = {}
    for row in rows:
        try:
            mmsi = int(row['mmsi'])
            at = timestamp(row['msgtime'])
            lat, lon = float(row['latitude']), float(row['longitude'])
            speed = row['speed_over_ground']
            speed = float(speed) if speed is not None else None
            if not (1 <= mmsi <= 999999999 and -90 <= lat <= 90 and -180 <= lon <= 180):
                raise ValueError()
            if speed is not None and (not math.isfinite(speed) or speed < 0 or speed >= 102.3):
                speed = None
            groups.setdefault((mmsi, at), set()).add((lat, lon, speed))
        except (ValueError, TypeError, KeyError):
            stats['invalid_rows'] += 1
    events = []
    for (mmsi, at), observations in sorted(groups.items()):
        if len(observations) != 1:
            stats['conflicting_timestamps'] += 1
            # An unknown observation breaks a stay, even if nearby samples exist.
            events.append((mmsi, at, None, None, None))
        else:
            events.append((mmsi, at, *next(iter(observations))))
    stats['unique_timestamps'] = len(events)
    visits = []
    active = None
    previous = None

    def finish(reason, departure=None):
        nonlocal active
        if active:
            seconds = (active['last_observed_at'] - active['arrival_at']).total_seconds()
            if active['observation_count'] >= 2 and seconds >= min_stay:
                visit = dict(active, departure_at=departure, end_reason=reason, observed_stay_seconds=seconds)
                visit['visit_id'] = digest([visit['mmsi'], visit['port_id'], visit['arrival_at']])
                visits.append(visit)
            else:
                stats['short_candidates'] += 1
        active = None

    for mmsi, at, lat, lon, speed in events:
        same_vessel = previous is not None and previous[0] == mmsi
        gap = same_vessel and (at-previous[1]).total_seconds() > max_gap
        if not same_vessel:
            finish('window_end')
        elif gap:
            finish('data_gap')

        candidate_ports = (
            []
            if lat is None
            else nearby_ports(lat, lon, port_grid, max_port_radius)
        )
        candidates = [
            (distance_m(lat, lon, port), port['port_id'])
            for port in candidate_ports
        ]

        inside = sorted((d, pid) for d, pid in candidates if d <= radii[pid])
        port_id = inside[0][1] if inside else None
        eligible = port_id if speed is not None and speed <= max_speed else None
        if port_id:
            stats['positions_inside_ports'] += 1
        if active and active['port_id'] != eligible:
            outside = lat is not None and active['port_id'] not in {pid for _, pid in inside}
            finish('observed_exit' if outside else 'movement_or_unknown', at if outside else None)
        if eligible:
            if active is None:
                # Boundary is known only if a recent preceding sample was outside this circle.
                previous_inside = previous[2] if same_vessel and not gap else None
                active = dict(mmsi=mmsi, port_id=eligible, arrival_at=at, last_observed_at=at,
                              arrival_censored=int(previous_inside is None or eligible in previous_inside), observation_count=0)
            active['last_observed_at'] = at
            active['observation_count'] += 1
        previous = (mmsi, at, {pid for _, pid in inside} if lat is not None else None)
    finish('window_end')
    stats['visits'] = len(visits)
    return visits, dict(stats)


def summaries(visits):
    counts = Counter((v['mmsi'], v['port_id']) for v in visits)
    return [dict(mmsi=mmsi, port_id=port_id, visit_count=count)
            for (mmsi, port_id), count in sorted(counts.items())]
