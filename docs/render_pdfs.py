#!/usr/bin/env python3
"""Regenerate the English/Russian operational PDFs from their Markdown sources.

Dependencies: reportlab, pillow. Uses Arial (macOS) or DejaVu (Linux).
Run from any directory. Architecture is placed on a larger landscape page so
its labels remain readable; operational pages use A4 with embedded fonts.
"""
from pathlib import Path
import hashlib
import html
import re
import textwrap

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, A3, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate, Paragraph, Spacer, PageBreak,
    NextPageTemplate, Image, Table, TableStyle, Preformatted,
)
from reportlab.platypus.tableofcontents import TableOfContents

ROOT = Path(__file__).resolve().parent
REPO_URL = 'https://github.com/KonstantinLofichenko/ais-graph-analytics/blob/main/'
INK = colors.HexColor('#17324d')
ACCENT = colors.HexColor('#087e8b')
MARGIN = 42
WIDTH = A4[0] - 2 * MARGIN


def register_fonts():
    candidates = [
        (Path('/System/Library/Fonts/Supplemental'),
         ['Arial.ttf', 'Arial Bold.ttf', 'Arial Italic.ttf', 'Arial Bold Italic.ttf', 'Courier New.ttf']),
        (Path('/usr/share/fonts/truetype/dejavu'),
         ['DejaVuSans.ttf', 'DejaVuSans-Bold.ttf', 'DejaVuSans-Oblique.ttf',
          'DejaVuSans-BoldOblique.ttf', 'DejaVuSansMono.ttf']),
    ]
    for directory, files in candidates:
        if all((directory / f).exists() for f in files):
            for name, filename in zip(['Body', 'BodyBold', 'BodyItalic', 'BodyBoldItalic', 'Mono'], files):
                pdfmetrics.registerFont(TTFont(name, str(directory / filename)))
            pdfmetrics.registerFontFamily('Body', normal='Body', bold='BodyBold',
                                        italic='BodyItalic', boldItalic='BodyBoldItalic')
            return
    raise SystemExit('Install Arial or DejaVu fonts before rendering.')


def normalize(value):
    return value.translate(str.maketrans({'–': '-', '—': '-', '‑': '-', '−': '-'}))


def inline(value):
    value = html.escape(normalize(value))
    # Protect code spans and links from emphasis substitution.
    tokens = []
    def protect(markup):
        tokens.append(markup)
        return f'ZZTOKEN{len(tokens)-1}ZZ'
    value = re.sub(r'`([^`]+)`', lambda m: protect('<font name="Mono" size="8.3">' + m[1] + '</font>'), value)
    def link(match):
        label, target = match.groups()
        if not re.match(r'https?://', target):
            if target.startswith('#'):
                target = REPO_URL + 'docs/README.md' + target
            else:
                target = REPO_URL + str((Path('docs') / html.unescape(target)).as_posix())
                target = target.replace('docs/../', '')
        return protect(f'<link href="{target}" color="#087e8b">{label}</link>')
    value = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', link, value)
    value = re.sub(r'\*\*([^*]+)\*\*', r'<b>\1</b>', value)
    for i, token in enumerate(tokens):
        value = value.replace(f'ZZTOKEN{i}ZZ', token)
    return value


class Guide(BaseDocTemplate):
    def __init__(self, output, language):
        super().__init__(str(output), pagesize=A4, leftMargin=MARGIN, rightMargin=MARGIN,
                         topMargin=48, bottomMargin=44,
                         title='AIS Graph Analytics - ' + ('Operational Guide' if language == 'ENG' else 'Руководство'),
                         author='Konstantin Lofichenko')
        self.language = language
        self.addPageTemplates([
            PageTemplate(id='Body', pagesize=A4,
                         frames=[Frame(MARGIN, 44, WIDTH, A4[1]-92, leftPadding=0,
                                       rightPadding=0, topPadding=0, bottomPadding=0)], onPage=self.decorate),
            PageTemplate(id='Diagram', pagesize=landscape(A3),
                         frames=[Frame(MARGIN, 44, landscape(A3)[0]-2*MARGIN,
                                       landscape(A3)[1]-92, leftPadding=0, rightPadding=0,
                                       topPadding=0, bottomPadding=0)], onPage=self.decorate),
        ])

    def decorate(self, canvas, doc):
        width, height = doc.pageTemplate.pagesize
        canvas.setPageSize((width, height))
        canvas.saveState()
        canvas.setFont('Body', 8)
        canvas.setFillColor(INK)
        canvas.drawString(MARGIN, height-28, 'AIS GRAPH ANALYTICS')
        canvas.drawRightString(width-MARGIN, height-28, '2026-10-08 | ' + self.language)
        canvas.setStrokeColor(colors.HexColor('#d7e2e8'))
        canvas.line(MARGIN, height-34, width-MARGIN, height-34)
        canvas.setFillColor(colors.HexColor('#607384'))
        label = 'Operational documentation' if self.language == 'ENG' else 'Документация платформы'
        canvas.drawString(MARGIN, 24, label)
        canvas.drawRightString(width-MARGIN, 24, str(doc.page))
        canvas.restoreState()

    def afterFlowable(self, flowable):
        if isinstance(flowable, Paragraph) and flowable.style.name == 'H2':
            title = flowable.getPlainText()
            key = 'section-' + hashlib.sha1(title.encode()).hexdigest()[:12]
            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(title, key, 0, False)
            self.notify('TOCEntry', (0, title, self.page, key))


def render(language):
    source = ROOT / ('README.md' if language == 'ENG' else 'README.ru.md')
    styles = {
        'Body': ParagraphStyle('Body', fontName='Body', fontSize=9.5, leading=13.5,
                               textColor=INK, spaceAfter=7, splitLongWords=True,
                               bulletFontName='Body', bulletFontSize=9.5),
        'H2': ParagraphStyle('H2', fontName='BodyBold', fontSize=16, leading=20,
                             textColor=INK, spaceBefore=15, spaceAfter=10, keepWithNext=True),
        'H3': ParagraphStyle('H3', fontName='BodyBold', fontSize=11.3, leading=15,
                             textColor=ACCENT, spaceBefore=9, spaceAfter=6, keepWithNext=True),
        'Cell': ParagraphStyle('Cell', fontName='Body', fontSize=8.5, leading=11.5,
                               textColor=INK, splitLongWords=True),
        'Code': ParagraphStyle('Code', fontName='Mono', fontSize=7.4, leading=10,
                               textColor=INK, spaceBefore=4, spaceAfter=9,
                               backColor=colors.HexColor('#f0f4f7'), borderPadding=6),
        'Cover': ParagraphStyle('Cover', fontName='BodyBold', fontSize=30, leading=36,
                               textColor=INK, spaceAfter=20),
    }
    story = [Spacer(1, 75), Paragraph('AIS Graph<br/>Analytics', styles['Cover'])]
    subtitle = 'Architecture and operations' if language == 'ENG' else 'Архитектура и эксплуатация'
    story += [Paragraph(subtitle, styles['H3']), Spacer(1, 20),
              Paragraph('Konstantin Lofichenko<br/>2026-10-08', styles['Body']),
              Spacer(1, 24)]
    cover = ('Current live, historical and graph platform. Includes automatic PyFlink startup, '
             'durable checkpoint recovery, ClickHouse derived streams and the six-tab Metabase dashboard.'
             if language == 'ENG' else
             'Текущая live, historical и графовая платформа. Автоматический запуск PyFlink, '
             'durable checkpoints и восстановление, derived streams в ClickHouse, шесть tabs Metabase.')
    story += [Paragraph(cover, styles['Body']), PageBreak(),
              Paragraph('Contents' if language == 'ENG' else 'Содержание', styles['H3'])]
    toc = TableOfContents()
    toc.levelStyles = [ParagraphStyle('Contents', fontName='Body', fontSize=9,
                                      leading=12, spaceBefore=4, textColor=INK)]
    story += [toc, PageBreak()]
    lines = source.read_text().splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith('# '):
            i += 1
            continue
        if line.startswith('```'):
            block = []
            i += 1
            while i < len(lines) and not lines[i].startswith('```'):
                raw = normalize(lines[i]).expandtabs(4)
                # Visual wrapping preserves text; continuation indentation is presentation only.
                block.extend(textwrap.wrap(raw, width=107, expand_tabs=False,
                                           replace_whitespace=False, drop_whitespace=False,
                                           subsequent_indent='  ') or [''])
                i += 1
            story.append(Preformatted('\n'.join(block), styles['Code']))
            i += 1
            continue
        if line.startswith('!['):
            section_heading = story.pop() if isinstance(story[-1], Paragraph) and story[-1].style.name == 'H2' else None
            story += [NextPageTemplate('Diagram'), PageBreak()]
            if section_heading is not None:
                story.append(section_heading)
            heading = 'Architecture: current and planned paths' if language == 'ENG' else 'Архитектура: текущие и planned пути'
            story.append(Paragraph(heading, styles['H3']))
            image = Image(str(ROOT / 'assets/architecture.png'))
            available = landscape(A3)[0] - 2*MARGIN
            scale = min(available / image.imageWidth, 660 / image.imageHeight)
            image.drawHeight = image.imageHeight * scale
            image.drawWidth = image.imageWidth * scale
            story += [image, NextPageTemplate('Body'), PageBreak()]
            i += 1
            continue
        if line.startswith('## '):
            story.append(Paragraph(inline(line[3:]), styles['H2']))
            i += 1
            continue
        if line.startswith('###'):
            story.append(Paragraph(inline(line.lstrip('#').strip()), styles['H3']))
            i += 1
            continue
        if line.startswith('|'):
            rows = []
            while i < len(lines) and lines[i].strip().startswith('|'):
                cells = [x.strip() for x in lines[i].strip().strip('|').split('|')]
                if not all(re.match(r'^:?-+:?$', x) for x in cells):
                    rows.append([Paragraph(inline(x), styles['Cell']) for x in cells])
                i += 1
            columns = len(rows[0])
            # Three-column operational tables need more room for destination/topic names.
            widths = [WIDTH / columns] * columns
            table = Table(rows, colWidths=widths, repeatRows=1, hAlign='LEFT')
            table.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#e0edf0')),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f5f8fa')]),
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('LEFTPADDING', (0,0), (-1,-1), 7), ('RIGHTPADDING', (0,0), (-1,-1), 7),
                ('TOPPADDING', (0,0), (-1,-1), 6), ('BOTTOMPADDING', (0,0), (-1,-1), 6),
                ('LINEBELOW', (0,0), (-1,0), .6, colors.HexColor('#adc5d0')),
            ]))
            story += [table, Spacer(1, 10)]
            continue
        bullet = re.match(r'^(-|\d+\.)\s+(.*)', line)
        if bullet:
            text = bullet[2]
            i += 1
            while i < len(lines) and lines[i].strip() and not re.match(r'^(?:[-#|`!]|\d+\.)', lines[i].strip()):
                text += ' ' + lines[i].strip()
                i += 1
            story.append(Paragraph(inline(text), styles['Body'],
                                   bulletText='•' if bullet[1]=='-' else bullet[1]))
            continue
        paragraph = line
        i += 1
        while i < len(lines) and lines[i].strip() and not re.match(r'^(?:[#|`!\-]|\d+\.)', lines[i].strip()):
            paragraph += ' ' + lines[i].strip()
            i += 1
        story.append(Paragraph(inline(paragraph), styles['Body']))
    output = ROOT / f'AIS-Graph-Analytics-{language}.pdf'
    Guide(output, language).multiBuild(story)
    print(f'{output.name}: {source.name} -> PDF')


if __name__ == '__main__':
    register_fonts()
    for language in ('ENG', 'RUS'):
        render(language)
