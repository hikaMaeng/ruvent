"""Builds docs/proposals/event-server-redesign.pptx from code.

The deck explains the redesign proposal (event-server-redesign.md) with
diagrams drawn from native PowerPoint shapes, so every box stays editable.

    python docs/proposals/deck/build.py
"""

from __future__ import annotations

from pathlib import Path

from lxml import etree
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Emu, Inches, Pt

OUT = Path(__file__).resolve().parents[1] / "event-server-redesign.pptx"
FONT = "맑은 고딕"

# Palette: an ink-navy system with one amber signal and a teal for "healthy".
INK = RGBColor(0x14, 0x21, 0x3D)
INK_2 = RGBColor(0x2B, 0x3A, 0x5C)
PAPER = RGBColor(0xFF, 0xFF, 0xFF)
MIST = RGBColor(0xEE, 0xF1, 0xF6)
LINE = RGBColor(0xC5, 0xCC, 0xD8)
MUTED = RGBColor(0x5B, 0x67, 0x7D)
AMBER = RGBColor(0xF2, 0xA5, 0x1A)
AMBER_SOFT = RGBColor(0xFD, 0xF1, 0xD8)
TEAL = RGBColor(0x1F, 0x8A, 0x7E)
TEAL_SOFT = RGBColor(0xDD, 0xF1, 0xEE)
RED = RGBColor(0xC8, 0x3E, 0x4D)
RED_SOFT = RGBColor(0xF8, 0xE1, 0xE4)

W, H = Inches(13.333), Inches(7.5)
MARGIN = Inches(0.6)


# ----------------------------------------------------------------- helpers


def _fonts(run, size, bold=False, color=INK, italic=False):
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.italic = italic
    run.font.color.rgb = color
    run.font.name = FONT
    rpr = run._r.get_or_add_rPr()
    for tag in ("a:ea", "a:cs"):
        node = rpr.find(qn(tag))
        if node is None:
            node = etree.SubElement(rpr, qn(tag))
        node.set("typeface", FONT)


def text(slide, x, y, w, h, lines, size=16, color=INK, bold=False, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, spacing=4, name=None):
    """A text box. `lines` is a string or a list of strings / (text, opts) pairs.

    opts: size, bold, color, bullet (bool), italic.
    """
    box = slide.shapes.add_textbox(x, y, w, h)
    if name:
        box.name = name
    frame = box.text_frame
    frame.word_wrap = True
    frame.margin_left = frame.margin_right = 0
    frame.margin_top = frame.margin_bottom = 0
    frame.vertical_anchor = anchor
    items = [lines] if isinstance(lines, str) else lines
    for index, item in enumerate(items):
        content, opts = (item, {}) if isinstance(item, str) else item
        paragraph = frame.paragraphs[0] if index == 0 else frame.add_paragraph()
        paragraph.alignment = align
        paragraph.space_after = Pt(opts.get("after", spacing))
        bullet = opts.get("bullet", False)
        run = paragraph.add_run()
        run.text = ("• " if bullet else "") + content
        _fonts(run, opts.get("size", size), opts.get("bold", bold), opts.get("color", color), opts.get("italic", False))
    return box


def box(slide, x, y, w, h, label="", fill=MIST, line=None, color=INK, size=14, bold=False, shape=MSO_SHAPE.ROUNDED_RECTANGLE, align=PP_ALIGN.CENTER, sub=None, name=None):
    """A filled shape with centered text. `sub` adds a smaller second line."""
    node = slide.shapes.add_shape(shape, x, y, w, h)
    if name:
        node.name = name
    node.fill.solid()
    node.fill.fore_color.rgb = fill
    if line is None:
        node.line.fill.background()
    else:
        node.line.color.rgb = line
        node.line.width = Pt(1.25)
    node.shadow.inherit = False
    if shape == MSO_SHAPE.ROUNDED_RECTANGLE:
        node.adjustments[0] = 0.12
    frame = node.text_frame
    frame.word_wrap = True
    frame.margin_left = frame.margin_right = Inches(0.08)
    frame.margin_top = frame.margin_bottom = Inches(0.04)
    frame.vertical_anchor = MSO_ANCHOR.MIDDLE
    first = frame.paragraphs[0]
    first.alignment = align
    run = first.add_run()
    run.text = label
    _fonts(run, size, bold, color)
    if sub:
        for line_text in sub if isinstance(sub, list) else [sub]:
            paragraph = frame.add_paragraph()
            paragraph.alignment = align
            run = paragraph.add_run()
            run.text = line_text
            _fonts(run, max(size - 3, 10), False, color)
    return node


def arrow(slide, x1, y1, x2, y2, color=INK_2, width=1.75, dashed=False, both=False):
    connector = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, x1, y1, x2, y2)
    connector.line.color.rgb = color
    connector.line.width = Pt(width)
    ln = connector.line._get_or_add_ln()
    if dashed:
        dash = etree.SubElement(ln, qn("a:prstDash"))
        dash.set("val", "dash")
    tail = etree.SubElement(ln, qn("a:tailEnd"))
    tail.set("type", "triangle")
    tail.set("w", "med")
    tail.set("len", "med")
    if both:
        head = etree.SubElement(ln, qn("a:headEnd"))
        head.set("type", "triangle")
    return connector


def table(slide, x, y, w, rows, widths, size=12, header_fill=INK, row_h=0.42):
    shape = slide.shapes.add_table(len(rows), len(rows[0]), x, y, w, Inches(row_h * len(rows)))
    grid = shape.table
    total = sum(widths)
    for index, ratio in enumerate(widths):
        grid.columns[index].width = Emu(int(w * ratio / total))
    for r, row in enumerate(rows):
        grid.rows[r].height = Inches(row_h)
        for c, value in enumerate(row):
            cell = grid.cell(r, c)
            cell.margin_left = cell.margin_right = Inches(0.08)
            cell.margin_top = cell.margin_bottom = Inches(0.03)
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            cell.fill.solid()
            cell.fill.fore_color.rgb = header_fill if r == 0 else (PAPER if r % 2 else MIST)
            frame = cell.text_frame
            frame.word_wrap = True
            paragraph = frame.paragraphs[0]
            run = paragraph.add_run()
            run.text = value
            _fonts(run, size, r == 0, PAPER if r == 0 else INK)
    return shape


def slide_base(prs, title, kicker=None, dark=False, notes=None):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    background = slide.background.fill
    background.solid()
    background.fore_color.rgb = INK if dark else PAPER
    if kicker:
        text(slide, MARGIN, Inches(0.42), Inches(9), Inches(0.3), kicker, size=12, color=AMBER if dark else TEAL, bold=True)
    text(slide, MARGIN, Inches(0.72), W - 2 * MARGIN, Inches(0.8), title, size=28, bold=True, color=PAPER if dark else INK)
    if notes:
        slide.notes_slide.notes_text_frame.text = notes
    return slide


def footer(slide, label, dark=False):
    text(slide, MARGIN, H - Inches(0.42), Inches(8), Inches(0.25), label, size=10, color=LINE if dark else MUTED)


def chip(slide, x, y, label, fill=AMBER_SOFT, color=INK, w=None):
    width = w or Inches(0.32 + 0.13 * len(label))
    return box(slide, x, y, width, Inches(0.34), label, fill=fill, color=color, size=11, bold=True)


# ----------------------------------------------------------------- slides


def build():
    prs = Presentation()
    prs.slide_width, prs.slide_height = W, H
    from slides import SLIDES  # noqa: E402  (slide content lives next to this file)

    helpers = dict(text=text, box=box, arrow=arrow, table=table, slide_base=slide_base, footer=footer, chip=chip, Inches=Inches, Pt=Pt, PP_ALIGN=PP_ALIGN, MSO_ANCHOR=MSO_ANCHOR, MSO_SHAPE=MSO_SHAPE, W=W, H=H, MARGIN=MARGIN,
                   INK=INK, INK_2=INK_2, PAPER=PAPER, MIST=MIST, LINE=LINE, MUTED=MUTED, AMBER=AMBER, AMBER_SOFT=AMBER_SOFT, TEAL=TEAL, TEAL_SOFT=TEAL_SOFT, RED=RED, RED_SOFT=RED_SOFT)
    for make in SLIDES:
        make(prs, helpers)
    prs.core_properties.title = "이벤트 서버 재설계 제안"
    prs.core_properties.subject = "ruvent"
    prs.save(OUT)
    return OUT


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    print(build())
