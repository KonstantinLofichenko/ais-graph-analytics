"""Assert the curated milestone counts, NULLs, schema and daily partition spec."""
import json
import os
import urllib.request
from pathlib import Path

if __package__:
    from .validate_raw import query
else:
    from validate_raw import query

REQUIRED = {'date_time', 'mmsi', 'longitude', 'latitude', 'navigational_status',
            'maneuver', 'data_source', 'ais_class', 'message_type', 'geometry'}


def validate():
    statements = Path(__file__).with_name('validate_curated.sql').read_text().split(';')
    results = [query(sql) for sql in statements if sql.strip()]
    counts, metrics, ddl, partitions, daily = results
    assert dict(counts) == {'raw': 23295797, 'curated': 23295797}, counts
    expected = [0, 2517680, 4511883, 50962, 4221794, 0, 0, 0, 0]
    assert metrics == [expected], metrics
    with urllib.request.urlopen(
        os.environ.get('ICEBERG_REST_URL', 'http://127.0.0.1:8181').rstrip('/') + '/v1/namespaces/hais_curated/tables/ais_positions', timeout=30
    ) as response:
        metadata = json.load(response)['metadata']
    schema = next(s for s in metadata['schemas'] if s['schema-id'] == metadata['current-schema-id'])
    fields = schema['fields']
    assert {f['name'] for f in fields if f['required']} == REQUIRED, fields
    expected_types = [
        ('date_time', 'timestamptz'), ('mmsi', 'int'), ('longitude', 'decimal(9, 6)'),
        ('latitude', 'decimal(8, 6)'), ('navigational_status', 'int'),
        ('course_over_ground', 'decimal(5, 2)'), ('true_heading', 'int'),
        ('speed_over_ground', 'decimal(5, 2)'), ('rate_of_turn', 'int'),
        ('maneuver', 'int'), ('data_source', 'string'), ('ais_class', 'string'),
        ('message_type', 'int'), ('geometry', 'binary'),
    ]
    assert [(f['name'], f['type']) for f in fields] == expected_types, fields
    spec = next(s for s in metadata['partition-specs'] if s['spec-id'] == metadata['default-spec-id'])
    timestamp_id = fields[0]['id']
    assert len(spec['fields']) == 1, spec
    assert spec['fields'][0]['transform'] == 'day' and spec['fields'][0]['source-id'] == timestamp_id, spec
    assert metadata['location'].startswith('s3://warehouse/hais_curated/'), metadata['location']
    report = {'counts': dict(counts), 'required_null_rows': metrics[0][0],
                      'normalized_nulls': dict(zip(['course_over_ground', 'true_heading',
                                                   'speed_over_ground', 'rate_of_turn'], metrics[0][1:5])),
                      'partition_spec': spec, 'schema': fields,
                      'partition_count': len(partitions), 'validation': 'passed'}
    return report, ddl[0][0], daily


def main():
    report, ddl, daily = validate()
    print(json.dumps(report, indent=2))
    print('Trino SHOW CREATE TABLE:', ddl)
    print('Daily aggregation:', daily)


if __name__ == '__main__':
    main()
