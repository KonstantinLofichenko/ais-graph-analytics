#!/usr/bin/env python3
"""Download WPI port reference data; fail rather than publish partial data."""

import argparse
import json
import math
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from detect import validate_ports


ROOT = Path(__file__).resolve().parents[2]

DEFAULT_SOURCE = (
    'https://gis.unocha.org/server/rest/services/Hosted/'
    'global_world_seaport_index_202511/FeatureServer/0/query'
)

DEFAULT_COUNTRY = 'NO'

# Current Bergen pilot defaults.
DEFAULT_MIN_LAT = 60.0
DEFAULT_MAX_LAT = 61.0
DEFAULT_MIN_LON = 4.0
DEFAULT_MAX_LON = 6.0

DEFAULT_RADIUS_M = 1500


def env_float(name, default):
    """Read and validate a finite float environment variable."""
    value = float(os.getenv(name, default))

    if not math.isfinite(value):
        raise ValueError(f'{name} must be a finite number')

    return value


def load_config():
    """Load port-source configuration from the project .env file."""
    load_dotenv(ROOT / '.env')

    source = os.getenv('PORTS_SOURCE_URL', DEFAULT_SOURCE).strip()
    country = os.getenv('PORTS_COUNTRY', DEFAULT_COUNTRY).strip().upper()

    min_lat = env_float('PORTS_MIN_LAT', DEFAULT_MIN_LAT)
    max_lat = env_float('PORTS_MAX_LAT', DEFAULT_MAX_LAT)
    min_lon = env_float('PORTS_MIN_LON', DEFAULT_MIN_LON)
    max_lon = env_float('PORTS_MAX_LON', DEFAULT_MAX_LON)

    if not source:
        raise ValueError('PORTS_SOURCE_URL must not be empty')

    if not country:
        raise ValueError('PORTS_COUNTRY must not be empty')

    if not -90 <= min_lat <= 90:
        raise ValueError('PORTS_MIN_LAT must be between -90 and 90')

    if not -90 <= max_lat <= 90:
        raise ValueError('PORTS_MAX_LAT must be between -90 and 90')

    if not -180 <= min_lon <= 180:
        raise ValueError('PORTS_MIN_LON must be between -180 and 180')

    if not -180 <= max_lon <= 180:
        raise ValueError('PORTS_MAX_LON must be between -180 and 180')

    if min_lat >= max_lat:
        raise ValueError('PORTS_MIN_LAT must be less than PORTS_MAX_LAT')

    if min_lon >= max_lon:
        raise ValueError('PORTS_MIN_LON must be less than PORTS_MAX_LON')

    return {
        'source': source,
        'country': country,
        'min_lat': min_lat,
        'max_lat': max_lat,
        'min_lon': min_lon,
        'max_lon': max_lon,
    }


def download():
    config = load_config()

    where = (
        f"country='{config['country']}' "
        f"AND latitude >= {config['min_lat']} "
        f"AND latitude <= {config['max_lat']} "
        f"AND longitude >= {config['min_lon']} "
        f"AND longitude <= {config['max_lon']}"
    )

    print(
        'Port reference configuration: '
        f"country={config['country']} "
        f"lat={config['min_lat']}..{config['max_lat']} "
        f"lon={config['min_lon']}..{config['max_lon']}"
    )

    response = requests.get(
        config['source'],
        params={
            'where': where,
            'outFields': 'index_no,port_name,country,latitude,longitude',
            'returnGeometry': 'false',
            'f': 'json',
        },
        timeout=(10, 60),
    )

    response.raise_for_status()

    body = response.json()

    if body.get('error'):
        raise RuntimeError(
            f"Port source returned an error: {body['error']}"
        )

    if body.get('exceededTransferLimit'):
        raise RuntimeError(
            'Port source truncated the result; refusing to publish partial data'
        )

    features = body.get('features')

    if not isinstance(features, list):
        raise RuntimeError('Port source response does not contain a features list')

    ports = []

    for feature in features:
        attributes = feature['attributes']

        ports.append(
            {
                'port_id': 'WPI:' + str(int(attributes['index_no'])),
                'name': attributes['port_name'],
                'country': attributes['country'],
                'latitude': float(attributes['latitude']),
                'longitude': float(attributes['longitude']),
                'radius_m': DEFAULT_RADIUS_M,
            }
        )

    validate_ports(ports)

    return sorted(ports, key=lambda port: port['port_id'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        '--output',
        type=Path,
        required=True,
        help='Destination JSON file',
    )

    args = parser.parse_args()

    ports = download()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Write atomically so a failed download never leaves a partially written file.
    temp = args.output.with_suffix(args.output.suffix + '.tmp')

    temp.write_text(
        json.dumps(ports, indent=2) + '\n',
        encoding='utf-8',
    )

    temp.replace(args.output)

    print(f'Downloaded {len(ports)} ports')


if __name__ == '__main__':
    main()