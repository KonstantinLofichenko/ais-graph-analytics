"""Opt-in live DB check: disposable ClickHouse database and rolled-back Neo4j transaction."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import sys
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run
from detect import detect, summaries


def main():
    run.load_dotenv(run.ROOT / '.env')
    database = 'port_visit_test_' + uuid.uuid4().hex
    ch = run.ClickHouse()
    original_query = ch.query
    original_query('CREATE DATABASE ' + database)
    try:
        ch.query = lambda sql, params=None: original_query(sql.replace('analytics.', database+'.').replace('DATABASE IF NOT EXISTS analytics', 'DATABASE IF NOT EXISTS '+database), params)
        ch.initialize()
        now = datetime(2026,9,1,tzinfo=timezone.utc)
        port = dict(port_id='TEST:'+uuid.uuid4().hex,name='Synthetic test',country='XX',latitude=60.,longitude=5.,radius_m=1500.)
        ch.upsert_ports([port], 'test', now)
        ch.upsert_ports([port], 'test', now+timedelta(hours=1))
        stamp = ch.query("SELECT created_at = toDateTime64('2026-09-01 00:00:00',6,'UTC'), updated_at = toDateTime64('2026-09-01 01:00:00',6,'UTC') FROM analytics.ports FINAL").strip()
        assert stamp == '1\t1', stamp
        rows = [dict(mmsi=999999998,msgtime=(now+timedelta(minutes=t)).isoformat(),
                     latitude=60. if t not in (22,51) else 61.,longitude=5.,speed_over_ground=.5)
                for t in (0,10,20,22,30,40,50,51)]
        visits,stats = detect(rows,[port])
        assert len(visits)==2
        counts=summaries(visits)
        payload=[dict(v,run_id='test',updated_at=now) for v in visits]
        ch.insert('port_visits',payload)
        ch.insert('port_visits',payload)
        assert ch.query('SELECT count() FROM analytics.port_visits FINAL').strip()=='2'
        ch.insert('port_visit_runs',[dict(run_id='test',window_start=now,window_end=now+timedelta(days=1),
                  dataset_hash='test',parameters='{}',source_rows=len(rows),visit_count=2,completed_at=now)])
        assert ch.query('SELECT count() FROM analytics.current_port_visits').strip()=='2'
        run.OWNER='test:'+uuid.uuid4().hex
        with run.GraphDatabase.driver(os.getenv('NEO4J_URI','bolt://localhost:7687'),
                                      auth=(os.getenv('NEO4J_USER','neo4j'),os.environ['NEO4J_PASSWORD'])) as driver:
            with driver.session(database=os.getenv('NEO4J_DATABASE','neo4j')) as session:
                tx=session.begin_transaction()
                try:
                    run.graph_snapshot(tx,[port],counts,'test',now,now+timedelta(days=1))
                    # Simulate an older creation timestamp and confirm reload preserves it.
                    tx.run("MATCH (p:Port {portId:$id}) SET p.createdAt=datetime('2020-01-01T00:00:00Z')", id=port['port_id']).consume()
                    run.graph_snapshot(tx,[port],counts,'test',now,now+timedelta(days=1))
                    stamp = tx.run("MATCH (p:Port {portId:$id}) RETURN p.createdAt.year=2020 AS preserved, p.updatedAt > p.createdAt AS updated", id=port['port_id']).single()
                    assert stamp['preserved'] and stamp['updated']
                    record=tx.run('MATCH ()-[r:VISITED {managedBy:$owner}]->() RETURN count(r) AS n, sum(r.visitCount) AS visits',owner=run.OWNER).single()
                    assert (record['n'],record['visits'])==(1,2)
                    run.graph_snapshot(tx,[port],[],'empty',now,now+timedelta(days=1))
                    assert tx.run('MATCH ()-[r:VISITED {managedBy:$owner}]->() RETURN count(r) AS n',owner=run.OWNER).single()['n']==0
                finally:
                    tx.rollback()
                assert session.run('MATCH (p:Port {portId:$id}) RETURN count(p) AS n',id=port['port_id']).single()['n']==0
        print('PASS: two visits, one relationship with visitCount=2, repeat-run idempotence, empty refresh, rollback cleanup')
    finally:
        original_query('DROP DATABASE '+database)
        ch.session.close()


if __name__=='__main__':
    main()
