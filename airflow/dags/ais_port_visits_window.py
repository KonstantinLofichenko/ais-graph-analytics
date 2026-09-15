"""Pure start/end/max_rows resolution for the ais_port_visits DAG (no Airflow imports)."""
from datetime import datetime, timedelta, timezone


def _parse_timestamp(value, field_name):
    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be an ISO-8601 string')
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f'{field_name} is not a valid ISO-8601 timestamp') from None
    if parsed.tzinfo is None:
        raise ValueError(f'{field_name} must include timezone information')
    return parsed.astimezone(timezone.utc)


def _positive_int(value, field_name):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError(f'{field_name} must be a positive integer') from None
    if parsed <= 0:
        raise ValueError(f'{field_name} must be a positive integer')
    return parsed


def resolve_window(conf, dag_run_start_date, window_hours_env, max_rows_env):
    """Resolve (start, end, max_rows, source) from dag_run.conf or rolling-window defaults.

    conf: dag_run.conf dict (may be None/empty). Supported keys: start, end, max_rows.
    dag_run_start_date: used as the rolling-window end when start/end are omitted.
    window_hours_env / max_rows_env: raw PORT_VISITS_WINDOW_HOURS / PORT_VISITS_MAX_ROWS
    environment values, used as fallbacks.
    """
    conf = conf or {}
    start_raw = conf.get('start')
    end_raw = conf.get('end')
    max_rows_raw = conf.get('max_rows')

    if (start_raw is None) != (end_raw is None):
        raise ValueError('dag_run.conf must supply both start and end, or neither')

    if start_raw is not None:
        start = _parse_timestamp(start_raw, 'start')
        end = _parse_timestamp(end_raw, 'end')
        if start >= end:
            raise ValueError('start must be earlier than end')
        source = 'dag_run.conf'
    else:
        window_hours = _positive_int(window_hours_env, 'PORT_VISITS_WINDOW_HOURS')
        end = dag_run_start_date
        start = end - timedelta(hours=window_hours)
        source = 'rolling-default'

    if max_rows_raw is not None:
        max_rows = _positive_int(max_rows_raw, 'max_rows')
    else:
        max_rows = _positive_int(max_rows_env, 'PORT_VISITS_MAX_ROWS')

    return start, end, max_rows, source
