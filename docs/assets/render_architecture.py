"""Render the canonical architecture SVG and PNG (documentation only).

Run from any directory with Python 3.9+ and Pillow installed:
    python docs/assets/render_architecture.py
No network, runtime configuration, or application state is used.
"""
from html import escape
import math
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

WIDTH, HEIGHT, SCALE = 2100, 1510, 2
HERE = Path(__file__).resolve().parent
image = Image.new('RGB', (WIDTH * SCALE, HEIGHT * SCALE), 'white')
draw = ImageDraw.Draw(image)
svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
       '<title>AIS Graph Analytics: current live architecture and planned historical lakehouse</title>',
       '<desc>Raw, PyFlink and graph paths feed persistent ClickHouse tables and Metabase. Dashed recovery links represent host-mounted checkpoints, not AIS data. The historical lakehouse is planned.</desc>',
       '<rect width="100%" height="100%" fill="white"/>']
INK, EDGE = '#25324a', '#718098'
COLORS = {'kafka': '#edf3ff', 'flink': '#f3efff', 'store': '#ecfaf2', 'graph': '#eef6ff',
          'bi': '#fff0f5', 'support': '#f4f6fa', 'future': '#fff7e7', 'plain': '#f8fafc'}


def font(size, bold=False):
    candidates = (['/System/Library/Fonts/Supplemental/Arial Bold.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf']
                  if bold else ['/System/Library/Fonts/Supplemental/Arial.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'])
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size * SCALE)
    raise RuntimeError('Install Arial or DejaVu Sans to render diagram text')


def text(x, y, value, size=23, bold=False, anchor='middle', color=INK):
    # Coordinates use the text baseline in both vector and raster outputs.
    svg.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-family="Arial,DejaVu Sans,sans-serif" font-size="{size}" font-weight="{700 if bold else 400}" fill="{color}">{escape(value)}</text>')
    draw.text((x * SCALE, y * SCALE), value, font=font(size, bold), fill=color,
              anchor={'middle': 'ms', 'start': 'ls', 'end': 'rs'}[anchor])


def line(points, dashed=False, color=EDGE, arrow=True):
    svg.append('<polyline points="' + ' '.join(f'{x},{y}' for x, y in points) + f'" fill="none" stroke="{color}" stroke-width="2.5"' + (' stroke-dasharray="8 6"' if dashed else '') + '/>')
    if dashed:
        for (x1, y1), (x2, y2) in zip(points, points[1:]):
            length = math.hypot(x2 - x1, y2 - y1)
            for start in range(0, int(length), 14):
                end = min(start + 8, length)
                draw.line([(int((x1 + (x2-x1)*start/length)*SCALE), int((y1 + (y2-y1)*start/length)*SCALE)),
                           (int((x1 + (x2-x1)*end/length)*SCALE), int((y1 + (y2-y1)*end/length)*SCALE))], fill=color, width=5)
    else:
        draw.line([(x*SCALE, y*SCALE) for x,y in points], fill=color, width=5)
    if arrow:
        (x1,y1),(x2,y2)=points[-2:]
        angle=math.atan2(y2-y1,x2-x1)
        triangle=[(x2,y2)] + [(x2-13*math.cos(angle+d),y2-13*math.sin(angle+d)) for d in (-0.45,0.45)]
        svg.append('<polygon points="'+' '.join(f'{x},{y}' for x,y in triangle)+f'" fill="{color}"/>')
        draw.polygon([(x*SCALE,y*SCALE) for x,y in triangle],fill=color)


def box(x,y,w,h,title,body=(),kind='plain',size=23,dashed=False):
    svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{COLORS[kind]}" stroke="{EDGE}" stroke-width="2"'+(' stroke-dasharray="8 6"' if dashed else '')+'/>')
    draw.rounded_rectangle((x*SCALE,y*SCALE,(x+w)*SCALE,(y+h)*SCALE),radius=16*SCALE,fill=COLORS[kind],outline=None if dashed else EDGE,width=3)
    # Dashed boundaries also appear in PNG; square inset leaves rounded corners intact.
    if dashed:
        for a,b in [((x+17,y),(x+w-17,y)),((x+17,y+h),(x+w-17,y+h)),((x,y+17),(x,y+h-17)),((x+w,y+17),(x+w,y+h-17))]:
            line([a,b],dashed=True,color=EDGE,arrow=False)
    text(x+w/2,y+34,title,24,True)
    for n,value in enumerate(body):
        assert draw.textlength(value,font=font(size)) <= (w-24)*SCALE, (title,value)
        text(x+w/2,y+65+n*28,value,size)


text(40,48,'AIS Graph Analytics',36,True,'start')
text(40,87,'Current live runtime, persistent analytics and checkpoint recovery',24,False,'start')
text(2060,87,'Updated 2026-10-08',20,False,'end',EDGE)
text(40,112,'Both PyFlink jobs enrich reference codes from dbt seeds',18,False,'start',EDGE)
box(250,125,400,78,'BarentsWatch AIS',kind='plain')
box(800,125,400,78,'Python AIS producer',kind='plain')
box(1350,125,400,78,'Kafka: ais.positions',kind='kafka')
line([(650,164),(800,164)])
line([(1200,164),(1350,164)])
# One shared topic fans out into the four implemented consumers.
line([(1550,203),(1550,222),(270,222)],arrow=False)
line([(1550,222),(1830,222)],arrow=False)
for x,title in [(270,'RAW HISTORY'),(790,'GAP STREAM'),(1310,'FEATURE STREAM'),(1830,'GRAPH')]:
    line([(x,222),(x,285)])
    text(x,266,title,20,True)
box(40,285,460,160,'ClickHouse ingestion',('Kafka Engine + materialized view','Transport only','Persistent destination below'),kind='store')
box(560,285,460,160,'PyFlink gap detector',('MMSI ValueState + processing timers','600s pipeline-observed silence','AIS_GAP_DETECTED / AIS_GAP_ENDED'),kind='flink',size=22)
box(1080,285,460,160,'PyFlink feature job',('MMSI / AIS event time','5/15/30/60 min windows; 5 min slide','30s watermark / 60s idleness'),kind='flink',size=22)
box(1600,285,460,90,'Kafka Connect',('Neo4j sink / current vessel state',),kind='graph',size=22)
box(560,485,460,66,'Kafka: ais.vessel.gaps',kind='kafka')
box(1080,485,460,66,'Kafka: ais.vessel.features',kind='kafka')
line([(790,445),(790,485)])
line([(1310,445),(1310,485)])
line([(270,445),(270,580)])
line([(790,551),(790,580)])
line([(1310,551),(1310,580)])
box(40,580,460,88,'raw.ais_positions',('Persistent full AIS event history',),kind='store',size=22)
box(560,580,460,88,'analytics.ais_vessel_gap_events',('raw Kafka Engine + MV -> MergeTree',),kind='store',size=22)
box(1080,580,460,88,'analytics.ais_vessel_features',('raw Kafka Engine + MV -> MergeTree',),kind='store',size=22)
box(1600,425,460,190,'Neo4j + APOC + GDS',('Vessel / Port / Community nodes','VISITED / CONNECTED_TO / MEMBER_OF','PageRank + Louvain communities','Current graph; not raw AIS history'),kind='graph',size=21)
line([(1830,375),(1830,425)])
line([(270,668),(270,720)])
box(40,720,460,115,'dbt / daily analytics',('Marts, port visits and AI enrichment','Airflow-orchestrated batch processing'),kind='store',size=21)
box(1600,720,460,115,'ClickHouse graph outputs',('Versioned snapshots + dbt BI models','BI / ML / AI analytical inputs'),kind='store',size=21)
line([(1830,615),(1830,720)])
line([(500,752),(530,752),(530,692),(1570,692),(1570,560),(1600,560)])
text(1050,684,'Port visits / graph synchronization',18)
box(560,865,1500,125,'Metabase / persistent ClickHouse tables',('Near Real-Time Vessel Analytics  |  Near Real-Time AIS Gap Monitoring','Also: Ports on Map, Ports, Vessels, Anomalies & AI Insights'),kind='bi',size=23)
line([(790,668),(790,865)])
line([(1310,668),(1310,865)])
line([(1830,835),(1830,865)])
line([(500,792),(535,792),(535,928),(560,928)])
# Supporting layer uses dotted links; it is not a record-processing path.
box(40,1030,2020,182,'SUPPORTING RECOVERY LAYER - dotted links are state operations',kind='support',dashed=True)
box(70,1078,490,82,'Both PyFlink jobs above',('Checkpoint / restore',),kind='flink',size=21)
box(655,1078,700,82,'Host-mounted durable checkpoints',('./flink/checkpoints -> /opt/flink/checkpoints',),kind='support',size=22)
box(1450,1078,580,82,'Restore Kafka offsets, state and timers',('Gap ValueState + partial 5/15/30/60m windows',),kind='support',size=21)
line([(560,1119),(655,1119)],dashed=True)
line([(1355,1119),(1450,1119)],dashed=True)
text(1050,1190,'60s / EXACTLY_ONCE state / timeout 120s / max 1 / restart 10 x 10s / local-host durability; Kafka output at-least-once',20)
box(40,1250,2020,86,'IMPLEMENTED HISTORICAL INGESTION',('HAIS GeoParquet -> Airflow -> raw.hais_positions -> dbt normalization -> raw.ais_positions',),kind='plain',size=24)
box(40,1362,2020,100,'PLANNED HISTORICAL LAKEHOUSE - future, not deployed',('HAIS GeoParquet -> Airflow -> PySpark -> Iceberg -> Trino',),kind='future',size=26,dashed=True)
text(40,1493,'Solid arrows: data flow. Recovery is a supporting layer. Metabase never queries Kafka Engine transport tables.',20,False,'start',EDGE)
svg.append('</svg>')
(HERE/'architecture.svg').write_text('\n'.join(svg)+'\n')
image.save(HERE/'architecture.png')
print('Rendered architecture.svg and architecture.png')
