"""Builds docs/intro/ruvent-intro.pptx: ruvent for a developer who has never seen it.

    python docs/intro/build.py
    powershell -NoProfile -ExecutionPolicy Bypass -File docs/proposals/deck/render.ps1 \
        -Deck docs/intro/ruvent-intro.pptx -Out target/intro-render

Style rules (docs/intro/README.md): white page, black text, two semantic colours
only (rust = claimable work, teal = dispatch), titles are the slide's claim in
plain words, no eyebrow labels, no decorative shapes, numbers only where order
matters, real artifacts (code, config, report lines) instead of stat callouts.
Every number is copied from the file named next to it.
"""

from __future__ import annotations

import sys
from pathlib import Path

from lxml import etree
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.dml import MSO_LINE
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, MSO_AUTO_SIZE, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Inches, Pt

OUT = Path(__file__).resolve().parent / "ruvent-intro.pptx"
FONT = "맑은 고딕"
MONO = "Consolas"

BLACK = RGBColor(0x1A, 0x1A, 0x1A)
GRAY = RGBColor(0x6B, 0x6B, 0x6B)
RULE = RGBColor(0xCF, 0xCF, 0xCF)
CODE_BG = RGBColor(0xF4, 0xF4, 0xF2)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
RUST = RGBColor(0xB5, 0x4A, 0x26)    # claimable work
TEAL = RGBColor(0x23, 0x6E, 0x6C)    # dispatch
TINT_RUST = RGBColor(0xF7, 0xE7, 0xE0)
TINT_TEAL = RGBColor(0xE0, 0xEF, 0xEE)

W, H = 13.333, 7.5
L = 0.75          # left margin
CW = W - 2 * L    # content width


def inch(v: float):
    return Inches(v)


# ── text ────────────────────────────────────────────────────────────────────


def set_font(run, name: str = FONT) -> None:
    run.font.name = name
    rpr = run._r.get_or_add_rPr()
    for tag in ("a:ea", "a:cs"):
        old = rpr.find(qn(tag))
        if old is not None:
            rpr.remove(old)
        el = etree.SubElement(rpr, qn(tag))
        el.set("typeface", FONT if name == MONO else name)


def bulletize(paragraph, color: RGBColor = GRAY, indent: float = 0.22) -> None:
    ppr = paragraph._p.get_or_add_pPr()
    ppr.set("marL", str(int(Inches(indent))))
    ppr.set("indent", str(-int(Inches(indent))))
    clr = etree.SubElement(ppr, qn("a:buClr"))
    etree.SubElement(clr, qn("a:srgbClr")).set("val", str(color))
    etree.SubElement(ppr, qn("a:buFont")).set("typeface", "Arial")
    etree.SubElement(ppr, qn("a:buChar")).set("char", "–")


def fill_frame(tf, paras, size, color, bold, align, font, after):
    tf.word_wrap = True
    tf.auto_size = MSO_AUTO_SIZE.NONE
    for i, item in enumerate(paras):
        spec = item if isinstance(item, dict) else {"t": item}
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = spec.get("align", align)
        p.space_after = Pt(spec.get("after", after))
        if "line" in spec:
            p.line_spacing = spec["line"]
        for text, opts in spec.get("runs") or [(spec.get("t", ""), {})]:
            r = p.add_run()
            r.text = text
            r.font.size = Pt(opts.get("size", spec.get("size", size)))
            r.font.bold = opts.get("bold", spec.get("bold", bold))
            r.font.color.rgb = opts.get("color", spec.get("color", color))
            set_font(r, opts.get("font", spec.get("font", font)))
        if spec.get("bullet"):
            bulletize(p)


def text(slide, x, y, w, h, paras, size=15, color=BLACK, bold=False, align=PP_ALIGN.LEFT,
         anchor=MSO_ANCHOR.TOP, font=FONT, after=0, margin=0.0):
    tb = slide.shapes.add_textbox(inch(x), inch(y), inch(w), inch(h))
    tf = tb.text_frame
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = inch(margin)
    tf.vertical_anchor = anchor
    fill_frame(tf, [paras] if isinstance(paras, (str, dict)) else paras, size, color, bold, align, font, after)
    return tb


def code(slide, x, y, w, h, lines, size=12.5, anchor=MSO_ANCHOR.TOP):
    """A plain grey code block. Comment lines are grey, the rest black."""
    s = rect(slide, x, y, w, h, fill=CODE_BG)
    tf = s.text_frame
    tf.margin_left = tf.margin_right = inch(0.3)
    tf.margin_top = tf.margin_bottom = inch(0.22)
    tf.vertical_anchor = anchor
    paras = [{"t": ln or " ", "color": GRAY if ln.lstrip().startswith(("//", "#")) else BLACK} for ln in lines]
    fill_frame(tf, paras, size, BLACK, False, PP_ALIGN.LEFT, MONO, 2)
    return s


# ── shapes ──────────────────────────────────────────────────────────────────


def rect(slide, x, y, w, h, fill=None, line=None, lw=1.0, dash=False, kind=MSO_SHAPE.RECTANGLE):
    s = slide.shapes.add_shape(kind, inch(x), inch(y), inch(w), inch(h))
    if fill is None:
        s.fill.background()
    else:
        s.fill.solid()
        s.fill.fore_color.rgb = fill
    if line is None:
        s.line.fill.background()
    else:
        s.line.color.rgb = line
        s.line.width = Pt(lw)
        if dash:
            s.line.dash_style = MSO_LINE.DASH
    s.shadow.inherit = False
    return s


def node(slide, x, y, w, h, paras, fill=WHITE, line=BLACK, color=BLACK, size=14, lw=1.0, dash=False,
         align=PP_ALIGN.CENTER):
    s = rect(slide, x, y, w, h, fill=fill, line=line, lw=lw, dash=dash)
    tf = s.text_frame
    tf.margin_left = tf.margin_right = inch(0.12)
    tf.margin_top = tf.margin_bottom = inch(0.05)
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    fill_frame(tf, [paras] if isinstance(paras, (str, dict)) else paras, size, color, False, align, FONT, 0)
    return s


def line(slide, x1, y1, x2, y2, color=BLACK, width=1.25, dash=False, head=True):
    c = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, inch(x1), inch(y1), inch(x2), inch(y2))
    c.line.color.rgb = color
    c.line.width = Pt(width)
    if dash:
        c.line.dash_style = MSO_LINE.DASH
    if head:
        tail = etree.SubElement(c.line._get_or_add_ln(), qn("a:tailEnd"))
        tail.set("type", "triangle")
        tail.set("w", "sm")
        tail.set("len", "sm")
    return c


def hrule(slide, x, y, w, color=RULE, width=0.75):
    return line(slide, x, y, x + w, y, color=color, width=width, head=False)


def table(slide, x, y, widths, rows, header=None, size=14, row_h=0.56, header_colors=None):
    """Rows separated by hairlines. No zebra fills, no boxes."""
    cy = y
    if header:
        cx = x
        for i, (cell, w) in enumerate(zip(header, widths)):
            col = (header_colors or {}).get(i, GRAY)
            text(slide, cx, cy, w - 0.15, 0.4, cell, size=12.5, bold=True, color=col, anchor=MSO_ANCHOR.BOTTOM)
            cx += w
        cy += 0.48
        hrule(slide, x, cy, sum(widths), color=BLACK, width=1)
    for row in rows:
        cx = x
        for i, (cell, w) in enumerate(zip(row, widths)):
            spec = cell if isinstance(cell, dict) else {"t": cell}
            text(slide, cx, cy, w - 0.2, row_h, spec, size=spec.get("size", size), anchor=MSO_ANCHOR.MIDDLE,
                 bold=spec.get("bold", False), color=spec.get("color", BLACK), font=spec.get("font", FONT))
            cx += w
        cy += row_h
        hrule(slide, x, cy, sum(widths))
    return cy


# ── slide frame ─────────────────────────────────────────────────────────────


class Deck:
    def __init__(self) -> None:
        self.prs = Presentation()
        self.prs.slide_width = inch(W)
        self.prs.slide_height = inch(H)
        self.count = 0

    def slide(self, title, notes, subtitle=None):
        s = self.prs.slides.add_slide(self.prs.slide_layouts[6])
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = WHITE
        self.count += 1
        if title:
            text(s, L, 0.55, CW, 0.6, title, size=24, bold=True)
        if subtitle:
            text(s, L, 1.17, CW, 0.4, subtitle, size=14, color=GRAY)
        if self.count > 1:
            text(s, W - L - 1, 7.0, 1, 0.25, str(self.count), size=10, color=GRAY, align=PP_ALIGN.RIGHT)
        s.notes_slide.notes_text_frame.text = notes
        return s


# ── slides ──────────────────────────────────────────────────────────────────


def cover(d: Deck):
    s = d.slide(None, "이 덱은 ruvent를 처음 보는 개발자용이다. 큰 그림, 이벤트 두 종류, 착수 규칙, 보장과 대가 순서로 간다.")
    text(s, L, 2.3, CW, 1.0, "ruvent", size=54, bold=True, font=MONO)
    text(s, L, 3.35, 10.5, 1.2,
         "Rust로 쓴 이벤트 서버. 일감은 워커 하나에게 배정하고, 결과는 그 순간의 구독자에게 넘긴다.",
         size=22)
    hrule(s, L, 5.3, CW, color=BLACK, width=1)
    text(s, L, 5.45, CW, 0.9, [
        "처음 보는 개발자를 위한 소개 · 2026-11-27",
        "기준: 판본 5.1(wire v6, 저널 형식 7), 저장소 F:\\dev\\ruvent",
    ], size=13, color=GRAY, after=4)


def why(d: Deck):
    s = d.slide("PostgreSQL + PGMQ 위의 브로커를 데이터베이스 없는 서버 하나로 바꿨다",
                "원본은 ndx_business의 packages/agent다. 대응표 전체는 docs/origin.md에 있다.")
    colw = (CW - 0.6) / 2
    for i, (head, sub, items) in enumerate([
        ("이전: packages/agent", "TypeScript · PostgreSQL + PGMQ", [
            "브로커, 워커 서버, PGMQ, 클라이언트가 PostgreSQL 하나를 공유한다",
            "명령은 큐(PGMQ)로, 관측은 event_store 로그로 흐른다",
            "브로커가 클라이언트마다 커서를 저장하며 로그를 읽어 보낸다",
            "브로커가 action → 큐 표를 보고 기록된 사실을 큐에 복사한다",
        ]),
        ("지금: ruvent", "Rust · 자체 세그먼트 저널", [
            "서버 하나가 장부 하나를 메모리에 쥔다(단일 작성자)",
            "자체 저널로 재기동을 넘는다. 데이터베이스는 없다",
            "발송은 저장하지 않고 등록 순간 구독자에게 넘긴다",
            "용량과 가용성은 독립 서버를 늘려 얻는다",
        ]),
    ]):
        x = L + i * (colw + 0.6)
        text(s, x, 1.75, colw, 0.45, head, size=18, bold=True)
        text(s, x, 2.2, colw, 0.35, sub, size=13, color=GRAY)
        hrule(s, x, 2.7, colw, color=BLACK, width=1)
        text(s, x, 2.9, colw, 3.6, [{"t": t, "bullet": True} for t in items], size=15, after=12)


def big_picture(d: Deck):
    s = d.slide("클라이언트는 프록시를 거친다. 이벤트 서버에 붙는 것은 프록시와 워커뿐이다",
                "이벤트 서버는 TCP 위 wire v6 하나만 말한다. 클라이언트의 HTTP 같은 프로토콜은 프록시가 받는다. "
                "서버는 접속자가 프록시인지 워커인지 묻지 않고, 권한은 메서드 단위로만 준다.")
    node(s, L, 2.15, 2.1, 0.95, [{"t": "클라이언트", "bold": True}, {"t": "HTTP 등", "size": 12, "color": GRAY}])
    node(s, 3.65, 2.15, 2.6, 0.95, [{"t": "프록시", "bold": True}, {"t": "프로젝트가 만든다", "size": 12, "color": GRAY}])
    node(s, 3.65, 4.05, 2.6, 0.95, [{"t": "워커 서버", "bold": True}, {"t": "ruvent-client", "size": 12, "color": GRAY}])
    line(s, L + 2.12, 2.62, 3.6, 2.62)
    line(s, 6.27, 2.62, 7.55, 2.62)
    line(s, 6.27, 4.52, 7.55, 4.52)
    text(s, 6.3, 2.25, 1.2, 0.3, "wire v6", size=11, color=GRAY, align=PP_ALIGN.CENTER, font=MONO)
    text(s, 6.3, 4.15, 1.2, 0.3, "wire v6", size=11, color=GRAY, align=PP_ALIGN.CENTER, font=MONO)

    rect(s, 7.6, 1.7, 4.98, 3.75, line=BLACK, lw=1.5)
    text(s, 7.85, 1.85, 4.5, 0.35, "ruvent-server", size=14, bold=True, font=MONO)
    node(s, 7.85, 2.35, 4.48, 0.8, [{"t": "착수용 큐", "bold": True, "color": RUST},
                                    {"t": "완료 보고로 지운다 · 저널에 쓴다", "size": 12}],
         fill=TINT_RUST, line=None, align=PP_ALIGN.LEFT)
    node(s, 7.85, 3.3, 4.48, 0.8, [{"t": "발송", "bold": True, "color": TEAL},
                                   {"t": "등록 순간 구독 접속에 넘기고 지운다", "size": 12}],
         fill=TINT_TEAL, line=None, align=PP_ALIGN.LEFT)
    node(s, 7.85, 4.25, 4.48, 0.8, [{"t": "내장 워커", "bold": True},
                                    {"t": "같은 메서드를 소켓 없이 함수로 부른다", "size": 12}],
         line=GRAY, dash=True, align=PP_ALIGN.LEFT)
    text(s, L, 5.85, CW, 0.9, [
        "서버의 연결 수는 프록시 수 + 워커 서버 수다. 클라이언트 수와 무관하다.",
        "프록시 골조(ruvent-proxy)는 서버별 관찰 연결과 채널별 팬아웃만 준다. 인증·요청–응답·역압은 프로젝트가 짠다.",
    ], size=14, after=6)


def two_kinds(d: Deck):
    s = d.slide("이벤트는 두 종류다: 착수용은 일감, 발송용은 결과",
                "한 엔벨롭 형식을 쓰고 flags 비트 0으로 종류를 나눈다. 착수용은 최소 1회 실행이므로 워커 쪽 멱등이 필요하다.")
    rows = [
        ("받는 쪽", "참여한 워커 중 하나", "그 순간 채널을 구독 중인 접속 전부"),
        ("사라지는 때", "그 워커가 완료를 보고할 때", "구독 접속에 넘기는 순간"),
        ("저장", "저널에 남아 재기동을 넘는다", "저장하지 않는다"),
        ("놓쳤을 때", "lease가 끝나면 회수해 다시 배정한다", "끊겨 있던 접속은 못 받는다. 재전송 없음"),
        ("실행 횟수", "최소 1회. 워커의 업무 멱등이 필요하다", "등록 순간의 구독자에게 한 번"),
        ("쓰임", "API 중계, 계산, 다음 단계", "결과, 스트리밍 조각, 알림"),
    ]
    table(s, L, 1.7, [2.3, 4.95, 4.58], rows, header=["", "착수용", "발송용"], size=15, row_h=0.68,
          header_colors={1: RUST, 2: TEAL})


def envelope(d: Deck):
    s = d.slide("엔벨롭에는 서버가 그 값으로 행동하는 필드만 있다",
                "고정 머리 38바이트 뒤에 필요할 때만 붙는 필드가 온다. 이름 사전은 없다. "
                "레코드 종류(요청, 알림, 배정, 전달, 저널)마다 실리는 필드는 Layout이 정한다.")
    text(s, L, 1.7, CW, 0.35, "고정 머리 38바이트(폭은 바이트 수에 비례)", size=13, color=GRAY)
    x = L
    for name, n in (("id", 16), ("channel", 16), ("type", 2), ("flags", 2), ("fail", 2)):
        w = CW * n / 38
        node(s, x, 2.1, w, 0.75, [{"t": name, "font": MONO, "size": 12 if n == 16 else 9.5},
                                  {"t": f"{n}", "size": 10, "color": GRAY}], line=BLACK, lw=0.75)
        x += w
    defs = [
        ("id", "UUIDv7. 논리 id이고 회수·재기동에도 바뀌지 않는다"),
        ("channel", "이름 없는 UUIDv7. 보통 요청 하나가 채널 하나. 발송은 채널로만 나뉜다"),
        ("type", "u16. 서버가 착수용 이벤트를 워커에게 나눌 때만 쓴다. 번호의 뜻은 프로젝트가 정한다"),
        ("flags", "비트 0 종류, 1 not_before, 2 워커 uuid. 나머지는 예약"),
        ("failures", "이벤트 탓인 실패 횟수(lease 만료, 실패 반환). 65,535에서 멈춘다"),
        ("+ 작업 키", "(세대, 도착 순번). 착수용의 대기 순서. 워커는 이것으로 중복 실행을 거른다"),
        ("+ 워커 uuid", "16바이트. 배정에만 실린다. 있으면 착수 중이다"),
        ("+ payload", "길이 4바이트 + 바이트. 서버는 읽지 않는다"),
    ]
    rows = [({"t": k, "font": MONO, "size": 13.5, "color": RUST if "워커" in k else BLACK}, v) for k, v in defs]
    table(s, L, 3.05, [2.3, CW - 2.3], rows, size=13.5, row_h=0.47)


def lifecycle(d: Deck):
    s = d.slide("착수용 이벤트는 배정 → 실행 → 완료로 끝나고, 실패하면 대기로 돌아간다",
                "배정은 서버가 민다. 워커는 채널, 타입, 또는 둘로 참여(Join)하고 창(동시에 쥘 수)을 연다. "
                "서버는 자리가 남은 참여자에게 등록순으로 돌아가며 배정한다. 폴링은 없다.")
    steps = [
        ("등록", "작업 키를 받는다"),
        ("대기", "(채널, 타입)별 줄"),
        ("배정", "자리가 남은 참여자에게\n등록순으로"),
        ("실행", "lease 동안.\n연장할 수 있다"),
        ("완료", "지우고, 후속 이벤트를\n같은 스텝에 등록"),
    ]
    bw, gap = 2.0, 0.46
    for i, (name, desc) in enumerate(steps):
        x = L + i * (bw + gap)
        col = TEAL if name == "완료" else (RUST if name in ("배정", "실행") else BLACK)
        node(s, x, 1.9, bw, 0.8, {"t": name, "bold": True, "color": col, "size": 17}, line=col, lw=1.5)
        text(s, x, 2.82, bw, 0.8, desc, size=13, color=GRAY, align=PP_ALIGN.CENTER)
        if i < 4:
            line(s, x + bw + 0.04, 2.3, x + bw + gap - 0.04, 2.3)
    run_cx = L + 3 * (bw + gap) + bw / 2
    wait_cx = L + 1 * (bw + gap) + bw / 2
    line(s, run_cx, 3.65, run_cx, 4.3, color=RUST)
    node(s, 5.6, 4.3, 5.0, 1.05, [
        {"t": "반환: 워커 uuid를 지운다. 실패면 실패 횟수 +1", "size": 13.5},
        {"t": "lease 만료: 회수하고 실패 횟수 +1", "size": 13.5},
    ], line=RUST, align=PP_ALIGN.LEFT)
    line(s, 5.6, 4.82, wait_cx, 4.82, color=RUST, head=False)
    line(s, wait_cx, 4.82, wait_cx, 3.65, color=RUST)
    text(s, L, 5.85, CW, 0.9, [
        {"t": "되돌아간 이벤트는 id가 그대로이고, 다른 워커가 착수할 수 있다.", "bullet": True},
        {"t": "서버는 실패 횟수를 세기만 한다. 몇 번에서 포기할지는 핸들러가 Context::failures()를 보고 정한다.", "bullet": True},
    ], size=14, after=6)


def claim_rule(d: Deck):
    s = d.slide("판본 5.1부터 착수는 (이벤트 id, 워커 uuid) 한 쌍으로만 식별한다",
                "착수 표지나 착수 마크가 따로 없다. 엔벨롭에 워커 uuid가 있느냐 없느냐가 착수 여부다. "
                "보고는 두 값이 다 맞아야 반영되고, 안 맞으면 stale로 세기만 한다.")

    def env(y, title, worker):
        text(s, L, y, 6, 0.32, title, size=13, color=GRAY)
        x = L
        for name, w in (("id", 1.1), ("channel", 1.2), ("type·flags·failures", 2.3)):
            node(s, x, y + 0.38, w, 0.6, {"t": name, "font": MONO, "size": 11}, line=BLACK, lw=0.75)
            x += w
        if worker:
            node(s, x, y + 0.38, 1.4, 0.6, {"t": "worker uuid", "font": MONO, "size": 11, "color": WHITE},
                 fill=RUST, line=RUST)
        else:
            node(s, x, y + 0.38, 1.4, 0.6, {"t": "없음", "size": 11.5, "color": GRAY}, line=RULE, dash=True)

    env(1.8, "대기 중", False)
    env(3.0, "착수 중", True)
    text(s, L, 4.35, 5.6, 1.5, [
        {"t": "워커 uuid는 Worker 하나에 하나. SDK가 채운다.", "bullet": True},
        {"t": "lease는 엔벨롭이 아니라 서버의 착수 기록에 있다.", "bullet": True},
    ], size=13.5, after=6)

    x0 = 6.95
    rules = [
        ("착수 요청에는 워커 uuid가 있어야 한다",
         "참여(Join)와 명시 착수(Claim)가 워커 uuid를 싣는다. 비어 있으면(nil) 거절한다."),
        ("보고는 두 값이 다 맞아야 반영된다",
         "완료·연장·반환은 이벤트 id와 워커 uuid를 보낸다. 하나라도 다르면 stale로 세고 아무것도 바꾸지 않는다."),
        ("맞는 실패 보고는 자리를 비운다",
         "워커 uuid를 지우고 실패 횟수를 1 올린다. 그 뒤 다른 워커가 착수할 수 있다."),
    ]
    y = 1.8
    for head, body in rules:
        text(s, x0, y, CW + L - x0, 0.38, head, size=15.5, bold=True)
        text(s, x0, y + 0.42, CW + L - x0, 0.9, body, size=13.5)
        y += 1.45
        hrule(s, x0, y - 0.18, CW + L - x0)
    text(s, L, 6.15, CW, 0.6,
         "대가: 같은 워커가 lease가 끝난 이벤트를 다시 배정받으면 옛 실행의 늦은 완료도 받는다. 워커 uuid는 인증이 아니다.",
         size=12.5, color=GRAY)


def request_reply(d: Deck):
    s = d.slide("요청–응답은 요청마다 만든 채널 하나로 묶는다",
                "어느 요청의 답인가는 채널이 정한다. 프록시가 요청마다 새 채널을 만들어 먼저 구독하고 그 채널에 일을 등록한다. "
                "워커는 타입으로 참여해 받고 같은 채널에 결과를 낸다. 실제 예는 apibondrust다.")
    cols = [("클라이언트", 1.5), ("프록시", 4.0), ("이벤트 서버", 6.65), ("워커", 9.3), ("추론 API 서버", 11.8)]
    for name, cx in cols:
        node(s, cx - 1.0, 1.55, 2.0, 0.5, {"t": name, "bold": True, "size": 13}, line=BLACK, lw=1)
        line(s, cx, 2.07, cx, 6.25, color=RULE, width=1, dash=True, head=False)
    xs = dict(cols)
    msgs = [
        ("클라이언트", "프록시", "API 요청", BLACK),
        ("프록시", "이벤트 서버", "새 채널 C 구독", BLACK),
        ("프록시", "이벤트 서버", "C에 일 등록(타입 CALL)", RUST),
        ("이벤트 서버", "워커", "배정", RUST),
        ("워커", "추론 API 서버", "실제 서버로 중계", BLACK),
        ("워커", "이벤트 서버", "응답 조각을 C로 발송", TEAL),
        ("이벤트 서버", "프록시", "구독자에게 즉시 넘김", TEAL),
        ("프록시", "클라이언트", "스트리밍 응답", TEAL),
        ("워커", "이벤트 서버", "완료 보고", RUST),
    ]
    for i, (a, b, lab, col) in enumerate(msgs):
        y = 2.5 + i * 0.43
        xa, xb = xs[a], xs[b]
        sgn = 1 if xb > xa else -1
        line(s, xa + 0.05 * sgn, y, xb - 0.05 * sgn, y, color=col, width=1.5)
        text(s, min(xa, xb), y - 0.3, abs(xb - xa), 0.27, f"{i + 1}. {lab}", size=11, color=col,
             align=PP_ALIGN.CENTER)
    text(s, L, 6.4, CW, 0.5,
         "6–8은 스트리밍 동안 반복된다. apibondrust는 OpenAI·Anthropic 요청을 여러 추론 서버로 나누는 중계기를 이 구조로 다시 세운 것이다.",
         size=12.5, color=GRAY)


def policy(d: Deck):
    s = d.slide("서버에 없는 정책은 워커와 프록시가 맡는다",
                "서버에 정책을 넣지 않은 것이 이 프로젝트의 핵심 결정이다. 근거는 docs/proposals/library-overhaul.md 9.15.")
    rows = [
        ("재시도 상한, 격리(dead letter)", "핸들러가 Context::failures()를 보고 그만둘 때 Ok(())로 완료한다. 필요하면 실패 후속을 함께 낸다"),
        ("순서", "앞 일을 처리한 워커가 다음 일을 후속 이벤트로 등록한다. 완료 보고와 같은 스텝에 등록된다"),
        ("만료", "워커가 payload나 failures를 보고 끝낸다. 서버에는 만료가 없다"),
        ("흐름 제어", "생산자의 미확인 바이트 상한(연결당 64 MiB). 가득 차면 send가 기다린다. 서버는 등록을 거절하지 않는다"),
        ("발송 재전송", "없다. 결과를 지켜야 하면 워커와 프로젝트가 정한다"),
        ("채널별 권한·범위", "없다. 권한은 메서드 단위뿐이다. 범위를 나누려면 서버를 나눈다"),
        ("payload 해석", "없다. 코어는 JSON을 모른다"),
    ]
    table(s, L, 1.65, [3.0, CW - 3.0], [({"t": a, "bold": True}, b) for a, b in rows],
          header=["서버에 없는 것", "대신 누가 어떻게"], size=14, row_h=0.66)


def engine(d: Deck):
    s = d.slide("엔진은 태스크 하나가 장부를 쥐는 단일 작성자 구조다",
                "락 대신 태스크 하나가 장부를 소유한다. State는 시각을 입력으로 받는 순수 상태 기계라서 소켓과 시계 없이 시험할 수 있다. "
                "부수 효과는 Effects로 내보내 각자의 스레드나 태스크가 처리한다.")
    node(s, L, 2.55, 2.0, 1.2, [{"t": "연결", "bold": True}, {"t": "생산 · 작업 · 관찰", "size": 11.5, "color": GRAY}])
    line(s, L + 2.02, 3.15, 3.25, 3.15)
    node(s, 3.3, 2.7, 1.9, 0.9, [{"t": "inbox", "font": MONO, "bold": True}, {"t": "mpsc", "size": 11.5, "color": GRAY, "font": MONO}])
    line(s, 5.22, 3.15, 5.75, 3.15)
    node(s, 5.8, 2.3, 2.5, 1.7, [{"t": "State", "font": MONO, "bold": True, "size": 18},
                                 {"t": "순수 상태 기계", "size": 12.5},
                                 {"t": "태스크·소켓·시계 없음", "size": 11.5, "color": GRAY}], lw=2)
    node(s, 5.8, 4.75, 2.5, 0.75, [{"t": "기한 휠", "bold": True}, {"t": "가장 이른 기한에 깨운다", "size": 11.5, "color": GRAY}])
    line(s, 7.05, 4.73, 7.05, 4.05)
    effects = ["연결별 송신 대기열", "로그 작성기", "본문 읽기", "체크포인트 작성기", "잡일(이주·삭제)", "스냅샷 수집기"]
    text(s, 9.3, 1.6, 3.3, 0.3, "Effects", size=12, color=GRAY, font=MONO)
    for k, e in enumerate(effects):
        y = 1.95 + k * 0.62
        node(s, 9.3, y, 3.28, 0.5, {"t": e, "size": 12.5}, line=RULE)
        line(s, 8.33, 3.15, 9.27, y + 0.25, color=GRAY, width=0.75)
    text(s, L, 5.95, CW, 0.9, [
        {"t": "서버는 어떤 접속의 송신도 기다리지 않는다. 느린 접속은 자기 대기열이 차면 끊긴다.", "bullet": True},
        {"t": "로그·체크포인트 쓰기나 본문 읽기가 실패하면 엔진은 사유를 남기고 멈춘다.", "bullet": True},
    ], size=14, after=6)


def journal(d: Deck):
    s = d.slide("저널은 세그먼트 파일과 체크포인트 하나로 재기동을 넘긴다",
                "미처리 착수용 이벤트의 순변화만 쓴다. 발송은 쓰지 않는다.")
    code(s, L, 1.7, 5.5, 2.3, [
        "data/journal/",
        "  lock              # 로그를 연 엔진 하나만",
        "  0000000001.seg",
        "  0000000002.seg",
        "  checkpoint.ckpt   # 마지막 완전한 체크포인트",
    ], size=12.5)
    text(s, L, 4.25, 5.5, 1.8, [
        {"t": "세그먼트에는 열이 셋 있다.", "after": 6},
        {"t": "로그: 레코드 프레임. 본문은 등록 레코드 안에", "bullet": True},
        {"t": "오래 남는 열: 이주한 본문", "bullet": True},
        {"t": "본문 열: 흘려 읽은 후속 본문", "bullet": True},
    ], size=13.5, after=3)
    rows = [
        ("Strict", "등록·완료·삭제의 성공 응답은 fsync 뒤에 간다. 착수는 기다리지 않는다"),
        ("체크포인트", "연산 묶음 사이에 장부를 조각씩 훑어 따로 쓴다. 서비스를 멈추지 않는다"),
        ("복구", "체크포인트에서 꼬리를 스트리밍으로 재생한다. 착수 중이던 것은 lease·워커 uuid와 함께 되살리고 실패로 세지 않는다"),
        ("손상", "마지막 세그먼트의 찢어진 꼬리만 잘라 낸다. 중간 손상은 멈추고 보고한다"),
        ("옛 형식", "형식 6 이하는 변환하지 않고 거부한다. 바이트는 그대로 둔다"),
    ]
    table(s, 6.75, 1.7, [1.6, CW + L - 6.75 - 1.6], [({"t": a, "bold": True}, b) for a, b in rows],
          size=13, row_h=0.9)


def guarantees(d: Deck):
    s = d.slide("서버가 약속하는 것은 여덟 가지이고, 각각 시험이 붙어 있다",
                "보장과 시험의 대응은 docs/testing.md의 '보장 ↔ 테스트' 표에 있다.")
    rows = [
        ("G1", "대기 중인 착수용 이벤트에 여럿이 착수하면 정확히 하나만 성공한다"),
        ("G2", "완료·실패 보고는 이벤트 id와 워커 uuid가 맞을 때만 반영된다. 경합에 진 보고와 완료 재전송은 후속과 함께 무효다"),
        ("G3", "작업 키는 등록 건마다 부여되고 회수·재기동에도 유지된다"),
        ("G4", "대기·착수 중인 착수용 이벤트는 저널 내구화 지점을 기준으로 재시작을 넘는다"),
        ("G5", "발송은 등록 처리 순간의 구독 접속에 넘겨지고 사라진다. 버퍼가 가득 찬 접속은 끊긴다"),
        ("G6", "서버는 어떤 접속의 송신도 기다리지 않는다"),
        ("G7", "삭제는 대기 중일 때만 성공하고, 결과 통지는 정확히 한 건이다"),
        ("G8", "착수용 큐에 있는 id로는 어떤 이벤트도 등록되지 않는다"),
    ]
    table(s, L, 1.65, [0.8, CW - 0.8], [({"t": g, "font": MONO, "color": GRAY}, t) for g, t in rows],
          size=14, row_h=0.6)


def code_taste(d: Deck):
    s = d.slide("워커 코드는 핸들러 하나다. 착수·연장·보고는 SDK가 한다",
                "Service::typed()는 어느 채널이든 그 타입의 일을 받는다. 핸들러는 Ok 또는 Err(Retry)만 돌려준다. "
                "연결(link)만 바꾸면 같은 코드가 프로세스 안 엔진에도, TCP 서버에도 붙는다.")
    code(s, L, 1.65, 8.0, 4.25, [
        "const CREATE: u16 = 10;   // 타입 번호는 프로젝트가 정한다",
        "const CREATED: u16 = 100;",
        "",
        "let worker = Worker::builder(link, &servers)",
        "    // 어느 채널이든 타입 CREATE. 동시에 16건까지",
        "    .service(Service::typed().slots(16).on(CREATE, |cx| async move {",
        "        if downstream_is_down() {",
        "            // 이벤트 탓이 아니다: 5초 뒤 반환, 실패 횟수 그대로",
        "            return Err(Retry::later(Duration::from_secs(5)));",
        "        }",
        "        // 같은 채널로 결과 발송. 완료 보고에 후속으로 실린다",
        "        cx.emit_dispatch(cx.channel(), CREATED, cx.payload().clone());",
        "        Ok(())   // 완료 보고: 이벤트 id + 이 워커의 uuid",
        "    }))",
        "    .start();",
        "",
        "worker.shutdown().await;   // 창 0 → 유예 → 남은 실행은 실패 아님으로 반환",
    ], size=11.5)
    text(s, 9.15, 1.65, CW + L - 9.15, 5.0, [
        {"t": "핸들러가 하지 않는 것", "bold": True, "after": 8},
        {"t": "착수 요청과 lease 연장", "bullet": True},
        {"t": "워커 uuid 채우기", "bullet": True},
        {"t": "완료·반환 보고", "bullet": True},
        {"t": "재접속(지수 백오프 + full jitter)", "bullet": True, "after": 18},
        {"t": "핸들러가 정하는 것", "bold": True, "after": 8},
        {"t": "언제 그만둘지(Context::failures())", "bullet": True},
        {"t": "결과를 어느 채널로 낼지", "bullet": True},
    ], size=13.5, after=5)


def run_server(d: Deck):
    s = d.slide("서버는 JSON 설정 하나로 뜬다",
                "listen과 credentials만 필수이고 나머지는 기본값이 있다. 권한은 자격마다 메서드 목록으로 준다. "
                "모든 키와 기본값은 crates/ruvent-server/README.md에 있다.")
    code(s, L, 1.65, 7.3, 3.75, [
        "// crates/ruvent-server/server.example.json 에서 발췌",
        "{",
        '  "listen": "127.0.0.1:7400",',
        '  "journal": "data/journal",',
        '  "durability": "strict",',
        '  "credentials": [',
        '    { "name": "workers", "token": "…",',
        '      "methods": ["register_claimable", "register_dispatch",',
        '                  "claim", "complete", "subscribe", "unsubscribe"] },',
        '    { "name": "ops", "token": "…", "methods": ["operate"] }',
        "  ],",
        '  "memory": { "limit": 8589934592, "high_watermark": 0.8 }',
        "}",
    ], size=11.5)
    code(s, L, 5.55, 7.3, 0.5, ["cargo run --release -p ruvent-server -- crates/ruvent-server/server.example.json"],
         size=10.5, anchor=MSO_ANCHOR.MIDDLE)
    x = 8.45
    w = CW + L - x
    text(s, x, 1.65, w, 4.8, [
        {"t": "메서드는 권한의 단위다", "bold": True, "after": 6},
        {"t": "일곱 개: 착수용 등록, 발송용 등록, 착수(claim), 완료(complete), 삭제, 구독, 구독 취소. 운영용 operate가 따로 있다.",
         "after": 14},
        {"t": "한 메서드에 호출이 여럿이다", "bold": True, "after": 6},
        {"t": "claim은 Join과 Claim, complete는 Complete·Extend·Release를 함께 연다.", "after": 14},
        {"t": "종료", "bold": True, "after": 6},
        {"t": "Ctrl-C면 drain 뒤 체크포인트를 쓰고 끝난다. 엔진이 스스로 멈추면 실패 코드로 끝난다."},
    ], size=13.5)


def crates(d: Deck):
    s = d.slide("저장소는 크레이트 여섯 개로 나뉜다",
                "코어는 ruvent 하나다. 서버, SDK, 프록시 골조가 그 위에 있다. 검증은 xtask 하나로 돌린다.")
    rows = [
        ("ruvent", "엔벨롭·채널·엔진·저널·운영 스냅샷, 메서드 계약과 인프로세스 연결", "모두"),
        ("ruvent-protocol", "이진 wire v6: 핸드셰이크, 호출·응답·알림·전달 레코드", "서버와 SDK 내부"),
        ("ruvent-server", "이벤트 서버 실행 파일. JSON 설정 하나", "운영"),
        ("ruvent-client", "원격 연결 SDK와 워커 프레임워크(Worker·Service·재시도)", "워커 서버, 프록시"),
        ("ruvent-proxy", "프록시 골조: 서버별 관찰 연결, 채널마다 상위 구독 하나의 팬아웃", "프록시 작성자"),
        ("xtask", "검증 관문 cargo xtask verify", "개발자"),
    ]
    table(s, L, 1.65, [2.9, 6.6, CW - 9.5],
          [({"t": c, "font": MONO, "size": 13.5}, a, {"t": b, "color": GRAY}) for c, a, b in rows],
          header=["크레이트", "하는 일", "쓰는 쪽"], size=14, row_h=0.68)


def verification(d: Deck):
    s = d.slide("마지막 cargo xtask verify에서 관문 11개가 모두 통과했다",
                "아래 줄은 docs/reports/test-2026-11-26-worker-uuid-claim.txt에서 그대로 옮겼다. "
                "시험 수 264는 같은 파일의 test result 줄을 더한 값이다.")
    code(s, L, 1.65, 6.6, 3.45, [
        "$ cargo xtask verify",
        "gate name=format status=ok seconds=0.7",
        "gate name=lint status=ok seconds=6.9",
        "gate name=api docs status=ok seconds=9.9",
        "gate name=static status=ok seconds=5.4",
        "gate name=state machine status=ok seconds=5.2",
        "gate name=runtime status=ok seconds=3.7",
        "gate name=acceptance status=ok seconds=2.3",
        "gate name=protocol status=ok seconds=5.8",
        "gate name=server status=ok seconds=10.0",
        "gate name=client and workers status=ok seconds=13.4",
        "gate name=proxy status=ok seconds=9.9",
        "verify status=ok",
    ], size=11.5)
    x = 7.75
    w = CW + L - x
    text(s, x, 1.65, w, 4.8, [
        {"t": "시험 264개 통과, 실패 0 (같은 실행)", "bold": True, "after": 14},
        {"t": "apibondrust(ruvent 위의 실제 응용)", "bold": True, "after": 4},
        {"t": "코드 변경 없이 빌드되고 시험 68개가 통과한다. 실제 프로세스 인수 시험은 최종 7/7.", "after": 14},
        {"t": "열린 문제", "bold": True, "color": RUST, "after": 4},
        {"t": "인수 시험의 첫 두 실행은 2/7이었다. 스트리밍이 약 2.5초 만에 빈 본문으로 끝났고 그 뒤 같은 스택이 멈췄다. "
              "v5 기준선과 이후 다섯 실행은 7/7이었고 재현 시험도 실패하지 않았다. 원인은 아직 모른다."},
    ], size=13.5)
    text(s, L, 5.35, CW, 0.5,
         "참고 측정(2026-11-23, 판본 5.1 이전 엔진, 64 태스크 인프로세스 등록→착수→완료): 휘발 249,143회/초, 파일 strict 33,134회/초. 다시 재지 않았다.",
         size=11.5, color=GRAY)


def constraints(d: Deck):
    s = d.slide("쓰기 전에 알아야 할 제약과 그 대가",
                "이 제약들은 결정이다. 바꾸려면 제안서의 해당 결정부터 다시 연다. 전체 목록은 docs/constraints.md에 있다.")
    rows = [
        ("착수용은 최소 1회 실행", "lease 만료·완료 유실·비정상 종료는 재실행을 낳는다. 워커의 일은 멱등해야 한다"),
        ("서버 하나 = 장부 하나", "서버 안 복제·분할이 없다. 디스크를 잃으면 그 서버의 대기 이벤트도 잃는다"),
        ("발송은 등록 순간의 구독자에게만", "구독자가 없거나 끊겨 있으면 사라진다"),
        ("워커 uuid는 인증이 아니다", "다른 워커의 uuid를 아는 자격은 그 착수를 완료·반환할 수 있다"),
        ("서버에 정책이 없다", "끝없이 실패하는 이벤트는 워커가 끝내지 않으면 계속 돈다"),
        ("앱 heartbeat가 없다", "조용한 끊김은 커널 keepalive로 알고, 약 30초 걸린다"),
        ("메타는 메모리에 남는다", "관리 메모리가 대기 수에 비례한다"),
    ]
    table(s, L, 1.65, [4.0, CW - 4.0], [({"t": a, "bold": True}, b) for a, b in rows],
          header=["제약", "대가"], size=14, row_h=0.64)


def first_day(d: Deck):
    s = d.slide("처음 하루에 할 일",
                "관문을 먼저 돌려 환경이 맞는지 보고, 문서는 overview부터 읽는다.")
    steps = [
        ("관문을 돌린다", "cargo xtask verify", "11개 관문이 모두 ok인지 본다. 마지막 줄이 verify status=ok가 아니면 환경부터 본다."),
        ("문서를 이 순서로 읽는다", "docs/overview.md → architecture.md → usage.md → constraints.md",
         "하는 것과 안 하는 것, 엔진·엔벨롭·저널, 쓰는 법, 결정된 대가 순서다."),
        ("요구와 시험을 맞춰 본다", "docs/testing.md", "바꾸려는 동작에 어떤 시험이 걸려 있는지 먼저 찾는다."),
    ]
    y = 1.75
    for i, (head, cmd, body) in enumerate(steps):
        text(s, L, y, 0.6, 0.5, f"{i + 1}", size=26, bold=True, color=RUST, font=MONO)
        text(s, L + 0.7, y + 0.02, CW - 0.7, 0.42, head, size=17, bold=True)
        text(s, L + 0.7, y + 0.5, CW - 0.7, 0.35, cmd, size=13.5, font=MONO)
        text(s, L + 0.7, y + 0.92, CW - 0.7, 0.4, body, size=13.5, color=GRAY)
        y += 1.55
        hrule(s, L, y - 0.18, CW)
    text(s, L, 6.45, CW, 0.4, "설계 근거와 사용자 원문: docs/proposals/library-overhaul.md", size=12.5, color=GRAY)


def main() -> None:
    d = Deck()
    for build in (cover, why, big_picture, two_kinds, envelope, lifecycle, claim_rule, request_reply,
                  policy, engine, journal, guarantees, code_taste, run_server, crates, verification,
                  constraints, first_day):
        build(d)
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else OUT   # another path while the deck is open in PowerPoint
    d.prs.save(out)
    print(f"wrote {out} ({d.count} slides)")


if __name__ == "__main__":
    main()
