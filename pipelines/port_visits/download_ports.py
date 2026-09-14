#!/usr/bin/env python3
"""Download the Bergen WPI pilot reference; fail rather than publish partial data."""
import argparse
import json
from pathlib import Path
import requests
from detect import validate_ports

SOURCE = 'https://gis.unocha.org/server/rest/services/Hosted/global_world_seaport_index_202511/FeatureServer/0/query'


def download():
    response = requests.get(SOURCE, params={
        'where': "country='NO' AND latitude >= 60 AND latitude <= 61 AND longitude >= 4 AND longitude <= 6",
        'outFields': 'index_no,port_name,country,latitude,longitude',
        'returnGeometry': 'false', 'f': 'json',
    }, timeout=(10, 60))
    response.raise_for_status()
    body = response.json()
    if body.get('error') or body.get('exceededTransferLimit'):
        raise RuntimeError('Port source returned an error or truncated data')
    ports = []
    for feature in body['features']:
        a = feature['attributes']
        ports.append(dict(port_id='WPI:'+str(int(a['index_no'])), name=a['port_name'],
                          country=a['country'], latitude=float(a['latitude']),
                          longitude=float(a['longitude']), radius_m=1500))
    validate_ports(ports)
    return sorted(ports, key=lambda p: p['port_id'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    ports = download()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_suffix('.tmp')
    temp.write_text(json.dumps(ports, indent=2)+'\n')
    temp.replace(args.output)
    print(f'Downloaded {len(ports)} ports')
