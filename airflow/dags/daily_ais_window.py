"""Date/row-limit configuration shared by the daily master and its local helper."""
from datetime import date, datetime, timedelta, timezone
import re


def resolve_activity_window(conf, run_started_at):
    conf = conf or {}
    if 'start' in conf or 'end' in conf:
        raise ValueError('Use activity_date (YYYY-MM-DD); start/end ranges are no longer accepted')
    if 'activity_date' in conf:
        value = conf['activity_date']
        error = 'dag_run.conf.activity_date must be a valid YYYY-MM-DD date'
        if not isinstance(value, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
            raise ValueError(error)
        try:
            activity_date = date.fromisoformat(value)
        except ValueError:
            raise ValueError(error) from None
    else:
        if run_started_at is None or run_started_at.utcoffset() is None:
            raise ValueError('A timezone-aware run start is required for the default activity_date')
        activity_date = run_started_at.astimezone(timezone.utc).date() - timedelta(days=1)

    window = {'activity_date': activity_date.isoformat(),
              'start': activity_date.isoformat(),
              'end': (activity_date + timedelta(days=1)).isoformat()}
    if conf.get('max_rows') is not None:
        value = conf['max_rows']
        if type(value) is not int or value <= 0:
            raise ValueError('max_rows must be a positive integer')
        window['max_rows'] = value
    return window
