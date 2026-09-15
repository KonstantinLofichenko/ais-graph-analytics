"""Pure aggregation of consecutive detected visits into directed port routes."""
from pipelines.port_visits.detect import timestamp


def aggregate_connections(visits):
    """Return deterministic route rows and counts for a single run's visits.

    first_seen is the earliest source arrival_at; last_seen is the latest
    destination arrival_at. Same-port visits remain in the sequence, but their
    transitions are ignored. Equal arrival times are ordered by visit_id.
    """
    ordered = sorted(
        (int(v['mmsi']), timestamp(v['arrival_at']), v['visit_id'], v['port_id'])
        for v in visits
    )
    routes = {}
    moving_vessels = set()
    previous = None
    for current in ordered:
        mmsi, arrival, _, port_id = current
        if previous is not None and previous[0] == mmsi and previous[3] != port_id:
            key = (previous[3], port_id)
            if key not in routes:
                routes[key] = dict(movement_count=0, vessels=set(),
                                   first_seen=previous[1], last_seen=arrival)
            route = routes[key]
            route['movement_count'] += 1
            route['vessels'].add(mmsi)
            route['first_seen'] = min(route['first_seen'], previous[1])
            route['last_seen'] = max(route['last_seen'], arrival)
            moving_vessels.add(mmsi)
        previous = current

    rows = [dict(from_port_id=source, to_port_id=destination,
                 movement_count=route['movement_count'], vessel_count=len(route['vessels']),
                 first_seen=route['first_seen'].isoformat(), last_seen=route['last_seen'].isoformat())
            for (source, destination), route in sorted(routes.items())]
    stats = dict(visits=len(ordered), movements=sum(row['movement_count'] for row in rows),
                 relationships=len(rows), vessels=len(moving_vessels))
    return rows, stats
