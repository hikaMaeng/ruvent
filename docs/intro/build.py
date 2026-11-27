"""Builds docs/intro/ruvent-intro.pptx: ruvent for a developer who has never seen it.

Every diagram is native PowerPoint shapes, so each box stays editable.

    python docs/intro/build.py
    powershell -NoProfile -ExecutionPolicy Bypass -File docs/proposals/deck/render.ps1 \
        -Deck docs/intro/ruvent-intro.pptx -Out target/intro-render

Numbers on the slides are copied from the report files named next to them.
"""

from __future__ import annotations

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

# Palette: ink for structure, rust for claimable work, teal for dispatch.
INK = RGBColor(0x1A, 0x21, 0x30)
INK_2 = RGBColor(0x2E, 0x38, 0x4C)
PAPER = RGBColor(0xFA, 0xF8, 0xF5)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
SAND = RGBColor(0xF1, 0xEB, 0xE3)
LINE = RGBColor(0xDD, 0xD5, 0xCA)
MUTED = RGBColor(0x66, 0x70, 0x85)
TEXT = RGBColor(0x1F, 0x26, 0x33)
RUST = RGBColor(0xC4, 0x51, 0x2A)
RUST_SOFT = RGBColor(0xF6, 0xE1, 0xD7)
TEAL = RGBColor(0x2D, 0x7A, 0x78)
TEAL_SOFT = RGBColor(0xDA, 0xEE, 0xEC)
ON_INK = RGBColor(0xC9, 0xD1, 0xDE)

W, H = 13.333, 7.5


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


def bulletize(paragraph, color: RGBColor, level_in: float = 0.22) -> None:
    ppr = paragraph._p.get_or_add_pPr()
    ppr.set("marL", str(int(Inches(level_in))))
    ppr.set("indent", str(-int(Inches(level_in))))
    for tag in ("a:buNone", "a:buChar", "a:buClr", "a:buFont"):
        old = ppr.find(qn(tag))
        if old is not None:
            ppr.remove(old)
    clr = etree.SubElement(ppr, qn("a:buClr"))
    srgb = etree.SubElement(clr, qn("a:srgbClr"))
    srgb.set("val", str(color))
    font = etree.SubElement(ppr, qn("a:buFont"))
    font.set("typeface", "Arial")
    char = etree.SubElement(ppr, qn("a:buChar"))
    char.set("char", "•")


def fill_frame(tf, paras, size, color, bold, align, font, space_after):
    tf.word_wrap = True
    tf.auto_size = MSO_AUTO_SIZE.NONE
    first = True
    for item in paras:
        spec = item if isinstance(item, dict) else {"t": item}
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        p.alignment = spec.get("align", align)
        p.space_after = Pt(spec.get("after", space_after))
        if spec.get("line"):
            p.line_spacing = spec["line"]
        runs = spec.get("runs") or [(spec.get("t", ""), {})]
        for text, opts in runs:
            r = p.add_run()
            r.text = text
            r.font.size = Pt(opts.get("size", spec.get("size", size)))
            r.font.bold = opts.get("bold", spec.get("bold", bold))
            r.font.color.rgb = opts.get("color", spec.get("color", color))
            set_font(r, opts.get("font", spec.get("font", font)))
        if spec.get("bullet"):
            bulletize(p, spec.get("bullet_color", RUST))


def text(slide, x, y, w, h, paras, size=14, color=TEXT, bold=False, align=PP_ALIGN.LEFT,
         anchor=MSO_ANCHOR.TOP, font=FONT, space_after=0, margin=0.0):
    box = slide.shapes.add_textbox(inch(x), inch(y), inch(w), inch(h))
    tf = box.text_frame
    m = inch(margin)
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = m
    tf.vertical_anchor = anchor
    if isinstance(paras, str):
        paras = [paras]
    fill_frame(tf, paras, size, color, bold, align, font, space_after)
    return box


# ── shapes ──────────────────────────────────────────────────────────────────


def box(slide, x, y, w, h, fill=WHITE, line=None, radius=None, dash=False, lw=1.0,
        shape=None):
    kind = shape or (MSO_SHAPE.ROUNDED_RECTANGLE if radius else MSO_SHAPE.RECTANGLE)
    s = slide.shapes.add_shape(kind, inch(x), inch(y), inch(w), inch(h))
    if radius and kind == MSO_SHAPE.ROUNDED_RECTANGLE:
        s.adjustments[0] = radius
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
    s.text_frame.text = ""
    return s


def label(shape, paras, size=14, color=TEXT, bold=False, align=PP_ALIGN.CENTER,
          anchor=MSO_ANCHOR.MIDDLE, font=FONT, margin=0.08, space_after=0):
    tf = shape.text_frame
    m = inch(margin)
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = m
    tf.vertical_anchor = anchor
    if isinstance(paras, str):
        paras = [paras]
    fill_frame(tf, paras, size, color, bold, align, font, space_after)


def arrow(slide, x1, y1, x2, y2, color=MUTED, width=1.5, dash=False, head=True):
    c = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, inch(x1), inch(y1), inch(x2), inch(y2))
    c.line.color.rgb = color
    c.line.width = Pt(width)
    if dash:
        c.line.dash_style = MSO_LINE.DASH
    if head:
        ln = c.line._get_or_add_ln()
        tail = etree.SubElement(ln, qn("a:tailEnd"))
        tail.set("type", "triangle")
        tail.set("w", "med")
        tail.set("len", "med")
    return c


def badge(slide, x, y, n, color=RUST, d=0.42, size=14):
    b = box(slide, x, y, d, d, fill=color, shape=MSO_SHAPE.OVAL)
    label(b, str(n), size=size, color=WHITE, bold=True, margin=0)
    return b


def card(slide, x, y, w, h, title, body, accent=RUST, num=None, fill=WHITE,
         title_size=17, body_size=13.5, title_color=None):
    box(slide, x, y, w, h, fill=fill, line=LINE, radius=0.06, lw=0.75)
    tx = x + 0.28
    if num is not None:
        badge(slide, x + 0.26, y + 0.26, num, color=accent)
        tx = x + 0.82
    text(slide, tx, y + 0.26, x + w - tx - 0.2, 0.42, title, size=title_size, bold=True,
         color=title_color or accent, anchor=MSO_ANCHOR.MIDDLE)
    if isinstance(body, str):
        body = [body]
    text(slide, x + 0.28, y + 0.86, w - 0.56, h - 1.0, body, size=body_size, color=TEXT,
         space_after=5)


# ── slide frame ─────────────────────────────────────────────────────────────


class Deck:
    def __init__(self) -> None:
        self.prs = Presentation()
        self.prs.slide_width = inch(W)
        self.prs.slide_height = inch(H)
        self.count = 0

    def blank(self, dark=False):
        s = self.prs.slides.add_slide(self.prs.slide_layouts[6])
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = INK if dark else PAPER
        self.count += 1
        return s

    def content(self, kicker, title, notes):
        s = self.blank()
        text(s, 0.6, 0.42, 10, 0.3, kicker, size=12, color=RUST, bold=True)
        text(s, 0.6, 0.72, 12.1, 0.62, title, size=26, color=INK, bold=True)
        text(s, 0.6, 7.02, 6, 0.28, "ruvent 소개", size=10, color=MUTED)
        text(s, 11.7, 7.02, 1.03, 0.28, str(self.count), size=10, color=MUTED, align=PP_ALIGN.RIGHT)
        s.notes_slide.notes_text_frame.text = notes
        return s


# ── slides ──────────────────────────────────────────────────────────────────


def cover(d: Deck):
    s = d.blank(dark=True)
    box(s, 0.9, 1.55, 0.9, 0.9, fill=RUST, shape=MSO_SHAPE.OVAL)
    text(s, 0.9, 2.75, 11, 1.2, "ruvent", size=66, color=WHITE, bold=True, font=MONO)
    text(s, 0.9, 4.0, 11.5, 0.6, "Rust로 만든 이벤트 서버와 워커·프록시 프레임워크", size=26, color=WHITE)
    text(s, 0.9, 4.75, 11.5, 0.9, [
        "처음 보는 개발자를 위한 소개",
        "판본 5.1 · wire v6 · 저널 형식 7 · 2026-11-27",
    ], size=15, color=ON_INK, space_after=4)
    s.notes_slide.notes_text_frame.text = (
        "ruvent는 일을 워커에게 맡기고 결과를 구독자에게 흘려보내는 작은 이벤트 서버다. "
        "이 덱은 프로젝트를 처음 보는 개발자가 30분 안에 큰 그림, 핵심 단어, 보장과 대가를 잡도록 짰다."
    )


def one_sentence(d: Deck):
    s = d.content("한 문장으로", "일을 맡기고, 결과를 흘려보내는 서버",
                  "이벤트는 두 종류뿐이다. 착수용은 일감, 발송용은 결과다. 그 밖의 정책은 서버 밖에 있다.")
    text(s, 0.6, 1.5, 12.1, 0.5,
         "ruvent 서버가 하는 일은 두 가지다. 나머지는 서버를 부르는 쪽의 몫이다.",
         size=16, color=MUTED)
    cols = [
        ("일을 맡긴다", RUST, [
            "착수용 이벤트.",
            "참여한 워커 하나에게만 배정되고, 그 워커가 완료를 보고하면 큐에서 지워진다.",
            "저널에 남아 서버 재기동을 넘는다.",
        ]),
        ("결과를 흘린다", TEAL, [
            "발송용 이벤트.",
            "등록되는 순간 그 채널을 구독 중인 접속에 넘기고 사라진다.",
            "저장하지 않는다.",
        ]),
        ("정책은 바깥에", INK_2, [
            "재시도 상한, 만료, 순서, 흐름 제어는 워커와 프록시가 정한다.",
            "서버는 실패 횟수를 세기만 한다.",
        ]),
    ]
    for i, (title, color, body) in enumerate(cols):
        x = 0.6 + i * 4.13
        card(s, x, 2.2, 3.85, 2.75, title, body, accent=color, num=i + 1, body_size=14.5)
    text(s, 0.6, 5.35, 12.1, 0.5,
         "서버는 접속자가 워커인지 프록시인지 묻지 않는다. 모두 같은 메서드를 부른다.",
         size=15, color=INK_2, bold=True)


def why(d: Deck):
    s = d.content("배경", "왜 만들었나: PostgreSQL 위의 브로커를 대체한다",
                  "원본은 ndx_business의 packages/agent다. 참가자 넷이 PostgreSQL 하나를 공유했다. "
                  "ruvent는 그 코어를 데이터베이스 없이 다시 썼다. 대응표는 docs/origin.md에 있다.")
    left = [
        {"t": "TypeScript · PostgreSQL + PGMQ", "color": MUTED, "after": 10},
        {"t": "브로커·워커 서버·PGMQ·클라이언트, 넷이 PostgreSQL 하나를 공유한다", "bullet": True, "bullet_color": MUTED},
        {"t": "명령은 큐(PGMQ), 관측은 event_store 로그. 경로가 둘이다", "bullet": True, "bullet_color": MUTED},
        {"t": "브로커가 클라이언트마다 커서를 저장하며 로그를 읽어 보낸다", "bullet": True, "bullet_color": MUTED},
        {"t": "브로커가 action → 큐 표로 기록된 사실을 큐에 복사한다", "bullet": True, "bullet_color": MUTED},
    ]
    right = [
        {"t": "Rust · 자체 세그먼트 저널", "color": MUTED, "after": 10},
        {"t": "서버 하나가 장부 하나를 메모리에 쥔다(단일 작성자)", "bullet": True},
        {"t": "데이터베이스 없이 자체 저널로 재기동을 넘는다", "bullet": True},
        {"t": "발송은 저장 없이 등록 순간 구독자에게 넘긴다", "bullet": True},
        {"t": "용량과 가용성은 독립 서버의 수로 얻는다", "bullet": True},
    ]
    box(s, 0.6, 1.65, 5.6, 4.3, fill=SAND, radius=0.05)
    text(s, 0.95, 1.95, 5.0, 0.45, "이전: packages/agent", size=19, bold=True, color=INK_2)
    text(s, 0.95, 2.5, 5.0, 3.9, left, size=15, space_after=9)
    arrow(s, 6.35, 3.8, 7.0, 3.8, color=RUST, width=2.5)
    box(s, 7.13, 1.65, 5.6, 4.3, fill=WHITE, line=RUST, radius=0.05, lw=1.5)
    text(s, 7.48, 1.95, 5.0, 0.45, "지금: ruvent", size=19, bold=True, color=RUST)
    text(s, 7.48, 2.5, 5.0, 3.9, right, size=15, space_after=9)


def big_picture(d: Deck):
    s = d.content("큰 그림", "누가 누구에게 붙는가",
                  "일반 클라이언트는 이벤트 서버에 직접 붙지 않는다. 클라이언트의 HTTP·WebSocket 같은 프로토콜은 프록시가 받는다. "
                  "이벤트 서버는 TCP 위 wire v6 하나만 말한다. 같은 프로세스 안의 내장 워커는 소켓 없이 같은 메서드를 함수로 부른다.")
    c = box(s, 0.6, 2.05, 2.2, 1.1, fill=SAND, radius=0.12)
    label(c, [{"t": "일반 클라이언트", "bold": True}, {"t": "HTTP 등", "size": 12, "color": MUTED}], size=15)
    p = box(s, 3.6, 2.05, 3.1, 1.1, fill=WHITE, line=INK_2, radius=0.12, lw=1.25)
    label(p, [{"t": "프록시", "bold": True}, {"t": "프로젝트가 만든다", "size": 12, "color": MUTED}, {"t": "골조: ruvent-proxy", "size": 12, "color": MUTED}], size=15)
    w = box(s, 3.6, 3.95, 3.1, 1.1, fill=WHITE, line=INK_2, radius=0.12, lw=1.25)
    label(w, [{"t": "워커 서버", "bold": True}, {"t": "ruvent-client: Worker·Service", "size": 12, "color": MUTED}], size=15)
    arrow(s, 2.85, 2.6, 3.55, 2.6, color=INK_2, width=2)
    arrow(s, 6.75, 2.6, 7.55, 2.6, color=INK_2, width=2)
    arrow(s, 6.75, 4.5, 7.55, 4.5, color=INK_2, width=2)
    text(s, 6.62, 2.18, 1.1, 0.3, "wire v6", size=11, color=MUTED, align=PP_ALIGN.CENTER)
    text(s, 6.62, 4.08, 1.1, 0.3, "wire v6", size=11, color=MUTED, align=PP_ALIGN.CENTER)

    box(s, 7.6, 1.6, 5.13, 4.0, fill=INK, radius=0.05)
    text(s, 7.9, 1.8, 4.6, 0.4, "이벤트 서버 · ruvent-server", size=16, color=WHITE, bold=True)
    q = box(s, 7.9, 2.35, 4.53, 0.9, fill=RUST, radius=0.1)
    label(q, [{"t": "착수용 큐", "bold": True}, {"t": "완료 보고로 제거 · 저널 대상", "size": 12}],
          size=14, color=WHITE, align=PP_ALIGN.LEFT, margin=0.15)
    f = box(s, 7.9, 3.38, 4.53, 0.9, fill=TEAL, radius=0.1)
    label(f, [{"t": "발송", "bold": True}, {"t": "등록 순간 구독 접속에 넘기고 제거", "size": 12}],
          size=14, color=WHITE, align=PP_ALIGN.LEFT, margin=0.15)
    e = box(s, 7.9, 4.41, 4.53, 0.9, fill=INK, line=ON_INK, radius=0.1, dash=True)
    label(e, [{"t": "내장 워커", "bold": True}, {"t": "같은 메서드를 소켓 없이 함수로 호출", "size": 12}],
          size=14, color=WHITE, align=PP_ALIGN.LEFT, margin=0.15)

    notes = [
        ("프로토콜은 하나", "이벤트 서버는 TCP 위 wire v6만 말한다. 프록시와 워커 서버만 그것으로 붙는다."),
        ("접속자를 가리지 않는다", "프록시든 워커든 같은 메서드를 부른다. 권한은 메서드 단위로만 준다."),
        ("연결 수가 작다", "서버의 연결 수는 프록시 수 + 워커 서버 수다. 클라이언트 수가 아니다."),
    ]
    for i, (t, b) in enumerate(notes):
        x = 0.6 + i * 4.13
        text(s, x, 5.9, 3.85, 0.35, t, size=14, bold=True, color=RUST)
        text(s, x, 6.25, 3.85, 0.7, b, size=12.5, color=TEXT)


def words(d: Deck):
    s = d.content("핵심 단어", "이 다섯 단어만 알면 나머지가 읽힌다",
                  "채널은 이름이 없다. 계층도 패턴도 없다. 타입은 서버가 일을 나눌 때만 쓴다. "
                  "워커 uuid는 판본 5.1에서 착수 여부를 나타내는 유일한 표시가 됐다.")
    rows = [
        ("이벤트 (엔벨롭)", RUST, "바이너리 한 덩어리. 고정 머리 38바이트(id · 채널 · 타입 · flags · 실패 횟수)에 서버가 읽지 않는 payload가 붙는다."),
        ("채널", TEAL, "이름 없는 UUIDv7(16바이트). 보통 요청 하나가 채널 하나다. 발송은 채널로만 나뉜다."),
        ("타입", INK_2, "u16 번호. 서버가 착수용 이벤트를 워커에게 나눌 때만 쓴다. 번호의 뜻은 프로젝트가 정한다."),
        ("워커 uuid", RUST, "착수한 워커의 이름표(16바이트). 엔벨롭에 있으면 착수 중, 없으면 대기다."),
        ("작업 키", INK_2, "(세대, 도착 순번). 대기 순서이고 재기동에도 그대로다. 중복 실행을 거르는 기준이다."),
    ]
    for i, (term, color, desc) in enumerate(rows):
        y = 1.62 + i * 1.0
        pill = box(s, 0.6, y, 2.5, 0.84, fill=color, radius=0.18)
        label(pill, term, size=15, color=WHITE, bold=True)
        box(s, 3.2, y, 9.53, 0.84, fill=WHITE, line=LINE, radius=0.12, lw=0.75)
        text(s, 3.45, y, 9.1, 0.84, desc, size=14.5, anchor=MSO_ANCHOR.MIDDLE)


def two_kinds(d: Deck):
    s = d.content("이벤트는 두 종류", "착수용은 일감, 발송용은 결과",
                  "한 엔벨롭 형식을 쓰고 flags 비트 0으로 종류를 나눈다. 착수용은 최소 1회 실행이므로 워커 쪽 멱등이 필요하다. "
                  "발송용은 재전송이 없다.")
    rows = [
        ("받는 쪽", "참여한 워커 중 하나", "그 순간 채널을 구독 중인 접속 전부"),
        ("사라지는 때", "그 워커가 완료를 보고할 때", "구독 접속에 넘기는 순간"),
        ("저장", "저널에 남아 재기동을 넘는다", "저장하지 않는다"),
        ("놓쳤을 때", "lease가 끝나면 회수해 다시 배정한다", "끊겨 있던 접속은 받지 못한다. 재전송 없음"),
        ("실행 횟수", "최소 1회. 워커의 업무 멱등이 필요하다", "등록 순간의 구독자에게 한 번"),
        ("주된 쓰임", "일감: API 중계, 계산, 다음 단계", "결과, 스트리밍 조각, 알림"),
    ]
    x0, wl, wc = 0.6, 2.3, 4.9
    hy = 1.65
    h1 = box(s, x0 + wl + 0.1, hy, wc, 0.65, fill=RUST, radius=0.1)
    label(h1, "착수용 이벤트", size=17, color=WHITE, bold=True)
    h2 = box(s, x0 + wl + 0.2 + wc, hy, wc, 0.65, fill=TEAL, radius=0.1)
    label(h2, "발송용 이벤트", size=17, color=WHITE, bold=True)
    for i, (k, a, b) in enumerate(rows):
        y = 2.42 + i * 0.73
        if i % 2 == 0:
            box(s, x0, y, wl + 0.2 + 2 * wc, 0.66, fill=SAND, radius=0.1)
        text(s, x0 + 0.2, y, wl - 0.2, 0.66, k, size=14.5, bold=True, color=INK_2, anchor=MSO_ANCHOR.MIDDLE)
        text(s, x0 + wl + 0.3, y, wc - 0.3, 0.66, a, size=14.5, anchor=MSO_ANCHOR.MIDDLE)
        text(s, x0 + wl + 0.4 + wc, y, wc - 0.3, 0.66, b, size=14.5, anchor=MSO_ANCHOR.MIDDLE)


def lifecycle(d: Deck):
    s = d.content("착수용 이벤트의 일생", "등록부터 완료까지, 그리고 되돌아가는 길",
                  "배정은 서버가 민다. 워커는 채널, 타입, 또는 둘로 참여하고 창(동시에 쥘 수)을 연다. "
                  "서버는 자리가 남은 참여자에게 등록순으로 돌아가며 배정한다. 상시 감시(폴링)가 없다.")
    steps = [
        ("등록", "생산자가 큐에 넣는다. 작업 키를 받는다"),
        ("대기", "(채널, 타입)별 줄에서 차례를 기다린다"),
        ("배정", "자리가 남은 참여자에게 등록순으로 돌아가며"),
        ("실행", "lease 동안 워커가 일한다. 연장할 수 있다"),
        ("완료", "지우고, 실린 후속 이벤트를 같은 스텝에 등록"),
    ]
    for i, (name, desc) in enumerate(steps):
        x = 0.6 + i * 2.5
        color = TEAL if name == "완료" else (RUST if name in ("배정", "실행") else INK_2)
        n = box(s, x, 1.75, 2.1, 0.95, fill=color, radius=0.15)
        label(n, name, size=19, color=WHITE, bold=True)
        text(s, x, 2.85, 2.1, 0.9, desc, size=13, color=TEXT, align=PP_ALIGN.CENTER)
        if i < 4:
            arrow(s, x + 2.13, 2.22, x + 2.47, 2.22, color=INK_2, width=2)
    # return path: from 실행 down, then back to 대기
    run_cx, wait_cx = 0.6 + 3 * 2.5 + 1.05, 0.6 + 2.5 + 1.05
    arrow(s, run_cx, 3.8, run_cx, 4.35, color=RUST, width=2)
    back = box(s, 5.55, 4.35, 4.6, 1.25, fill=RUST_SOFT, line=RUST, radius=0.1, lw=1)
    label(back, [
        {"t": "되돌림", "bold": True, "color": RUST},
        {"t": "반환: 워커 uuid를 지운다(실패면 실패 횟수 +1)", "size": 12.5},
        {"t": "lease 만료: 회수하고 실패 횟수 +1", "size": 12.5},
    ], size=14, align=PP_ALIGN.LEFT, margin=0.15)
    arrow(s, 5.55, 4.97, wait_cx, 4.97, color=RUST, width=2, head=False)
    arrow(s, wait_cx, 4.97, wait_cx, 3.8, color=RUST, width=2)
    text(s, 0.6, 5.95, 12.1, 0.9, [
        {"t": "논리 id는 회수해도 바뀌지 않는다. 다시 대기로 돌아간 이벤트는 다른 워커가 착수할 수 있다.", "bullet": True},
        {"t": "서버는 실패 횟수를 세기만 한다. 몇 번에서 포기할지는 워커가 엔벨롭의 failures를 보고 정한다.", "bullet": True},
    ], size=14, space_after=6)


def claim_rule(d: Deck):
    s = d.content("판본 5.1의 규칙", "착수 = (이벤트 id, 워커 uuid)",
                  "착수 표지나 착수 마크가 따로 없다. 엔벨롭에 워커 uuid가 있느냐 없느냐가 착수 여부다. "
                  "보고는 두 값이 다 맞아야 반영되고, 안 맞으면 stale로 세기만 한다.")
    text(s, 0.6, 1.6, 6.0, 0.4, "엔벨롭에서 보이는 차이", size=15, bold=True, color=INK_2)

    def env(y, title, with_worker):
        text(s, 0.6, y, 6.0, 0.35, title, size=13.5, color=MUTED, bold=True)
        parts = [("id", 1.25, INK_2), ("채널", 1.25, INK_2), ("타입·flags·실패", 1.55, INK_2)]
        x = 0.6
        for name, w, col in parts:
            b = box(s, x, y + 0.4, w - 0.05, 0.7, fill=col)
            label(b, name, size=12.5, color=WHITE, bold=True, margin=0.02)
            x += w
        if with_worker:
            b = box(s, x, y + 0.4, 1.45, 0.7, fill=RUST)
            label(b, "워커 uuid", size=12.5, color=WHITE, bold=True, margin=0.02)
        else:
            b = box(s, x, y + 0.4, 1.45, 0.7, fill=None, line=LINE, dash=True)
            label(b, "없음", size=12.5, color=MUTED, margin=0.02)

    env(2.1, "대기 중: 워커 uuid가 없다", False)
    env(3.45, "착수 중: 착수한 워커의 uuid가 있다", True)
    text(s, 0.6, 4.85, 5.9, 1.6, [
        {"t": "워커는 Worker 하나에 uuid 하나를 쓴다. SDK가 채운다.", "bullet": True},
        {"t": "lease는 엔벨롭이 아니라 서버의 착수 기록에 있다.", "bullet": True},
    ], size=13.5, space_after=6)

    rules = [
        ("착수 요청에는 워커 uuid가 필수", "참여(Join)와 명시 착수(Claim)는 워커 uuid를 싣는다. 비었으면(nil) 거절한다."),
        ("보고는 두 값이 다 맞아야 반영", "완료·연장·반환은 이벤트 id와 워커 uuid를 보낸다. 하나라도 다르면 stale로 세고 아무것도 바꾸지 않는다."),
        ("맞는 실패 보고는 자리를 비운다", "워커 uuid를 지우고 실패 횟수를 1 올린다. 그러면 다른 워커가 착수할 수 있다."),
    ]
    for i, (t, b) in enumerate(rules):
        y = 1.6 + i * 1.72
        card(s, 6.95, y, 5.78, 1.55, t, b, accent=RUST, num=i + 1, title_size=15.5, body_size=13)


def request_reply(d: Deck):
    s = d.content("요청–응답은 채널로", "예: API 요청 하나가 흐르는 길",
                  "어느 요청의 답인가는 채널이 정한다. 묻는 쪽(프록시)이 요청마다 새 채널을 만들어 먼저 구독하고, 그 채널에 일을 등록한다. "
                  "워커는 타입으로 참여해 받고 같은 채널에 결과를 낸다. 실제 예는 apibondrust다.")
    cols = [("클라이언트", 1.45), ("프록시", 3.95), ("이벤트 서버", 6.6), ("워커", 9.25), ("추론 API 서버", 11.75)]
    for name, cx in cols:
        dark = name == "이벤트 서버"
        h = box(s, cx - 1.0, 1.55, 2.0, 0.55, fill=INK if dark else SAND, radius=0.15)
        label(h, name, size=13.5, color=WHITE if dark else INK_2, bold=True)
        arrow(s, cx, 2.12, cx, 6.25, color=LINE, width=1.25, dash=True, head=False)
    xs = {n: cx for n, cx in cols}
    msgs = [
        ("클라이언트", "프록시", "API 요청", INK_2),
        ("프록시", "이벤트 서버", "새 채널 C 구독", INK_2),
        ("프록시", "이벤트 서버", "C에 일 등록(타입 CALL)", RUST),
        ("이벤트 서버", "워커", "배정: 착수 시작", RUST),
        ("워커", "추론 API 서버", "실제 서버로 중계", INK_2),
        ("워커", "이벤트 서버", "응답 조각을 C로 발송", TEAL),
        ("이벤트 서버", "프록시", "구독자에게 즉시 넘김", TEAL),
        ("프록시", "클라이언트", "스트리밍 응답", TEAL),
        ("워커", "이벤트 서버", "완료 보고", RUST),
    ]
    for i, (a, b, lab, col) in enumerate(msgs):
        y = 2.5 + i * 0.43
        xa, xb = xs[a], xs[b]
        sgn = 1 if xb > xa else -1
        arrow(s, xa + 0.06 * sgn, y, xb - 0.06 * sgn, y, color=col, width=1.75)
        lx = min(xa, xb)
        text(s, lx, y - 0.31, abs(xb - xa), 0.28, f"{i + 1}  {lab}", size=11, color=col,
             align=PP_ALIGN.CENTER, bold=True)
    text(s, 0.6, 6.42, 12.1, 0.5,
         "6–8은 스트리밍 동안 반복된다. 실제 예: apibondrust(OpenAI·Anthropic 요청을 여러 추론 서버로 나누는 중계기)를 ruvent 위에 다시 세웠다.",
         size=12.5, color=MUTED)


def not_doing(d: Deck):
    s = d.content("서버가 하지 않는 일", "서버는 작게, 정책은 부르는 쪽에",
                  "서버에 정책을 넣지 않은 것이 이 프로젝트의 핵심 결정이다. 대신 워커와 프록시가 그 일을 한다. "
                  "순서는 앞 일을 끝낸 워커가 다음 일을 등록하는 방식으로 성립한다.")
    items = [
        "재시도 상한, 격리(dead letter)",
        "만료",
        "순서 보장(그룹 직렬화)",
        "생산자 정지, 역압",
        "채널 이름·계층·패턴, 채널별 권한",
        "payload 해석(JSON을 모른다)",
        "발송 이벤트의 재전송",
    ]
    box(s, 0.6, 1.6, 5.4, 5.2, fill=SAND, radius=0.05)
    text(s, 0.95, 1.85, 4.8, 0.4, "서버에 없는 것", size=17, bold=True, color=INK_2)
    for i, it in enumerate(items):
        y = 2.45 + i * 0.58
        box(s, 0.98, y + 0.12, 0.2, 0.2, fill=RUST)
        text(s, 1.35, y, 4.5, 0.45, it, size=15, anchor=MSO_ANCHOR.MIDDLE)
    qa = [
        ("순서가 필요하면?", "앞 일을 처리한 워커가 다음 일을 후속 이벤트로 등록한다. 완료 보고와 같은 스텝에 등록되므로 순서가 성립한다."),
        ("끝없이 실패하면?", "엔벨롭의 실패 횟수를 워커가 보고 끝낸다. 서버는 세기만 하고 막지 않는다."),
        ("흐름을 늦추려면?", "생산자 쪽 미확인 버퍼와 운영이 맡는다. 서버는 생산자를 멈추거나 등록을 거절하지 않는다."),
    ]
    for i, (q, a) in enumerate(qa):
        card(s, 6.35, 1.6 + i * 1.77, 6.38, 1.6, q, a, accent=TEAL, num=i + 1, title_size=16, body_size=13.5)


def envelope(d: Deck):
    s = d.content("엔벨롭", "38바이트 머리와 필요할 때만 붙는 필드",
                  "엔벨롭에는 서버가 그 값으로 행동하는 필드만 있다. 이름 사전도 없다. "
                  "레코드 종류(요청, 알림, 배정, 전달, 저널)마다 실리는 필드는 Layout이 정한다.")
    text(s, 0.6, 1.6, 12, 0.35, "고정 머리 38바이트 (비율대로)", size=14, bold=True, color=INK_2)
    parts = [("id", 16, "16"), ("채널", 16, "16"), ("타입", 2, "2"), ("flags", 2, "2"), ("실패", 2, "2")]
    x, total = 0.6, 12.13
    for name, n, sz in parts:
        w = total * n / 38
        b = box(s, x, 2.0, w - 0.04, 1.0, fill=INK_2 if n == 16 else RUST)
        label(b, [{"t": name, "bold": True}, {"t": f"{sz} B", "size": 10.5}], size=13 if n == 16 else 11,
              color=WHITE, margin=0.01)
        x += w
    text(s, 0.6, 3.05, 12.1, 0.35,
         "id: UUIDv7  ·  채널: UUIDv7  ·  타입: u16  ·  flags 비트 0 종류, 1 not_before, 2 워커 uuid  ·  실패 횟수: u16(65,535에서 멈춤)",
         size=12, color=MUTED)
    text(s, 0.6, 3.65, 12, 0.35, "필요할 때만 붙는 필드", size=14, bold=True, color=INK_2)
    opt = [
        ("작업 키", "세대 4 + 도착 순번 varint", "착수용. 서버가 보내거나 기록할 때"),
        ("not_before", "varint", "선택. 착수용 이벤트를 늦게 시작할 때"),
        ("워커 uuid", "16 B", "배정에만. 착수한 워커"),
        ("payload", "길이 4 + 바이트", "항상. 서버는 읽지 않는다"),
    ]
    for i, (name, size, when) in enumerate(opt):
        x = 0.6 + i * 3.06
        col = RUST if name == "워커 uuid" else INK_2
        box(s, x, 4.05, 2.88, 2.2, fill=WHITE, line=LINE, radius=0.07, lw=0.75)
        text(s, x + 0.22, 4.25, 2.5, 0.4, name, size=16, bold=True, color=col,
             font=MONO if name in ("not_before", "payload") else FONT)
        text(s, x + 0.22, 4.7, 2.5, 0.35, size, size=12.5, color=MUTED)
        text(s, x + 0.22, 5.15, 2.5, 1.0, when, size=13.5)
    text(s, 0.6, 6.45, 12.1, 0.4, "와이어는 [tag][head_len][body_len][head][body]의 연속이고 바깥 프레임이 없다. 엔벨롭 머리는 코어 코덱 하나로 인코딩한다.",
         size=12.5, color=MUTED)


def engine(d: Deck):
    s = d.content("엔진 내부", "단일 작성자: 태스크 하나가 장부를 쥔다",
                  "락 대신 태스크 하나가 장부를 소유한다. State는 시각을 입력으로 받는 순수 상태 기계라서 소켓과 시계 없이 시험할 수 있다. "
                  "부수 효과는 Effects로 내보내 각자의 스레드·태스크가 처리한다.")
    c = box(s, 0.6, 2.55, 2.1, 1.5, fill=SAND, radius=0.1)
    label(c, [{"t": "연결들", "bold": True}, {"t": "생산 · 작업 · 관찰", "size": 12, "color": MUTED}], size=15)
    arrow(s, 2.75, 3.3, 3.25, 3.3, color=INK_2, width=2)
    i = box(s, 3.3, 2.75, 1.9, 1.1, fill=WHITE, line=INK_2, radius=0.1)
    label(i, [{"t": "inbox", "bold": True, "font": MONO}, {"t": "mpsc · recv_many", "size": 11.5, "color": MUTED}], size=15)
    arrow(s, 5.25, 3.3, 5.75, 3.3, color=INK_2, width=2)
    st = box(s, 5.8, 2.3, 2.6, 2.0, fill=INK, radius=0.08)
    label(st, [{"t": "State", "bold": True, "font": MONO, "size": 20},
               {"t": "순수 상태 기계", "size": 13}, {"t": "태스크·소켓·시계 없음", "size": 11.5, "color": ON_INK}],
          size=14, color=WHITE)
    wh = box(s, 5.8, 5.0, 2.6, 0.8, fill=WHITE, line=INK_2, radius=0.12)
    label(wh, [{"t": "기한 휠", "bold": True}, {"t": "가장 이른 기한만 알린다", "size": 11.5, "color": MUTED}], size=14)
    arrow(s, 7.1, 4.97, 7.1, 4.35, color=INK_2, width=1.75)
    effects = ["연결별 송신 대기열", "로그 작성기", "본문 읽기", "체크포인트 작성기", "잡일(이주·삭제)", "스냅샷 수집기"]
    text(s, 9.25, 1.55, 3.4, 0.3, "Effects", size=13, bold=True, color=MUTED, font=MONO)
    for k, e in enumerate(effects):
        y = 1.9 + k * 0.68
        b = box(s, 9.25, y, 3.48, 0.56, fill=WHITE, line=LINE, radius=0.15, lw=0.75)
        label(b, e, size=13)
        arrow(s, 8.45, 3.3, 9.2, y + 0.28, color=MUTED, width=1)
    text(s, 0.6, 6.05, 12.1, 0.8, [
        {"t": "서버는 어떤 접속의 송신도 기다리지 않는다(G6). 느린 접속은 그 접속의 대기열이 차면 끊긴다.", "bullet": True},
        {"t": "로그·체크포인트 쓰기나 본문 읽기가 실패하면 엔진은 멈추고 사유를 낸다. 틀린 장부로 계속 돌지 않는다.", "bullet": True},
    ], size=13.5, space_after=5)


def durability(d: Deck):
    s = d.content("내구성", "저널: 데이터베이스 없이 재기동을 넘는 방법",
                  "미처리 착수용 이벤트의 순변화만 쓴다. 발송은 쓰지 않는다. "
                  "Strict는 등록·완료·삭제의 성공을 디스크 기록 뒤에 알린다. 착수는 기다리지 않는다.")
    code = box(s, 0.6, 1.65, 5.6, 2.45, fill=INK, radius=0.04)
    label(code, [
        {"t": "data/journal/", "color": WHITE, "bold": True},
        {"t": "  lock              로그를 연 엔진 하나만", "color": ON_INK},
        {"t": "  0000000001.seg    세그먼트", "color": ON_INK},
        {"t": "  0000000002.seg    …", "color": ON_INK},
        {"t": "  checkpoint.ckpt   마지막 완전한 체크포인트", "color": ON_INK},
    ], size=13, font=MONO, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, margin=0.3, space_after=8)
    lanes = [("로그", "레코드 프레임. 본문은 등록 안에", RUST), ("오래 남는 열", "이주한 본문", INK_2), ("본문 열", "흘려 읽은 후속 본문", INK_2)]
    text(s, 0.6, 4.4, 5.6, 0.35, "세그먼트의 세 열(번호는 하나로 이어진다)", size=13, bold=True, color=INK_2)
    for k, (n, desc, col) in enumerate(lanes):
        x = 0.6 + k * 1.9
        b = box(s, x, 4.8, 1.8, 1.25, fill=WHITE, line=col, radius=0.1, lw=1.25)
        label(b, [{"t": n, "bold": True, "color": col}, {"t": desc, "size": 11, "color": MUTED}], size=13)
    pts = [
        ("Strict", "등록·완료·삭제의 성공 응답은 디스크 기록(fsync) 뒤에 간다. 착수는 기다리지 않는다."),
        ("퍼지 체크포인트", "연산 묶음 사이에 장부를 조각씩 훑어 따로 쓴다. 서비스를 멈추지 않는다."),
        ("복구", "체크포인트에서 꼬리를 스트리밍으로 재생한다. 착수 중이던 것은 lease·워커 uuid와 함께 되살린다."),
        ("손상", "마지막 세그먼트의 찢어진 꼬리만 잘라 낸다. 중간 손상은 멈추고 보고한다."),
        ("옛 형식", "형식 6 이하는 변환하지 않고 거부하며 바이트를 보존한다."),
    ]
    for k, (t, b) in enumerate(pts):
        y = 1.65 + k * 1.03
        text(s, 6.6, y, 6.1, 0.35, t, size=15, bold=True, color=RUST)
        text(s, 6.6, y + 0.36, 6.1, 0.65, b, size=13)


def guarantees(d: Deck):
    s = d.content("보장 계약", "서버가 약속하는 여덟 가지",
                  "각 보장에는 시험이 붙어 있다. docs/testing.md의 '보장 ↔ 테스트' 표에서 찾을 수 있다.")
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
    for i, (g, t) in enumerate(rows):
        y = 1.6 + i * 0.65
        if i % 2 == 0:
            box(s, 0.6, y, 12.13, 0.58, fill=SAND, radius=0.12)
        gb = box(s, 0.75, y + 0.09, 0.7, 0.4, fill=RUST if g in ("G1", "G2") else INK_2, radius=0.3)
        label(gb, g, size=12.5, color=WHITE, bold=True, margin=0)
        text(s, 1.7, y, 10.9, 0.58, t, size=13.5, anchor=MSO_ANCHOR.MIDDLE)


def code_taste(d: Deck):
    s = d.content("코드 맛보기", "워커 하나: 타입 CREATE의 일을 받아 결과를 발송한다",
                  "Service::typed()는 어느 채널이든 그 타입의 일을 받는다. 착수, lease 연장, 워커 uuid, 완료 보고는 SDK가 한다. "
                  "핸들러는 일만 하고 Ok 또는 Err(Retry)를 돌려준다. 연결 방법만 바꾸면 같은 코드가 TCP로도 돈다.")
    code = [
        "const CREATE: u16 = 10;    // 타입 번호는 프로젝트가 정한다",
        "const CREATED: u16 = 100;",
        "",
        "let worker = Worker::builder(local, &servers)",
        "    .service(Service::typed().slots(16).on(CREATE, |cx| async move {",
        "        if downstream_is_down() {",
        "            return Err(Retry::later(Duration::from_secs(5)));",
        "        }",
        "        cx.emit_dispatch(cx.channel(), CREATED, cx.payload().clone());",
        "        Ok(())",
        "    }))",
        "    .start();",
        "",
        "worker.shutdown().await;",
    ]
    b = box(s, 0.6, 1.6, 7.6, 5.2, fill=INK, radius=0.03)
    label(b, [{"t": ln or " ", "color": ON_INK if ln.strip().startswith("//") else WHITE} for ln in code],
          size=11.5, font=MONO, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.MIDDLE, margin=0.3, space_after=3)
    notes = [
        ("Service::typed().slots(16)", "어느 채널이든 타입 CREATE의 일을 받는다. 16은 동시에 쥘 일의 수(창)다."),
        ("Err(Retry::later(…))", "5초 뒤 반환한다. 이벤트 탓이 아니므로 실패 횟수는 그대로다."),
        ("cx.emit_dispatch(…)", "같은 채널로 결과를 발송한다. 완료 보고에 후속으로 실려 같은 스텝에 등록된다."),
        ("Ok(())", "완료 보고. 이벤트 id와 이 워커의 uuid를 SDK가 채워 보낸다."),
    ]
    for k, (t, desc) in enumerate(notes):
        y = 1.6 + k * 1.3
        badge(s, 8.55, y + 0.02, k + 1, color=RUST, d=0.38, size=13)
        text(s, 9.1, y, 3.63, 0.4, t, size=12.5, bold=True, color=RUST, font=MONO)
        text(s, 9.1, y + 0.42, 3.63, 0.85, desc, size=12.5)


def crates(d: Deck):
    s = d.content("저장소 지도", "크레이트 여섯 개",
                  "코어는 ruvent 하나다. 서버, SDK, 프록시 골조가 그 위에 있다. 검증은 xtask 하나로 돌린다.")
    rows = [
        ("ruvent", "엔벨롭·채널·엔진·저널·운영 스냅샷, 메서드 계약과 인프로세스 연결", "코어. 모두가 쓴다"),
        ("ruvent-protocol", "이진 wire v6: 핸드셰이크, 호출·응답·알림·전달 레코드", "서버와 SDK 내부"),
        ("ruvent-server", "이벤트 서버 실행 파일. JSON 설정 하나", "운영"),
        ("ruvent-client", "원격 연결 SDK와 워커 프레임워크(Worker·Service·재시도)", "워커 서버, 프록시"),
        ("ruvent-proxy", "프록시 골조: 서버별 관찰 연결, 채널마다 상위 구독 하나의 팬아웃", "프록시를 만드는 프로젝트"),
        ("xtask", "검증 관문 cargo xtask verify", "개발자"),
    ]
    hy = 1.6
    for xx, ww, t in ((0.6, 2.9, "크레이트"), (3.6, 6.2, "하는 일"), (9.9, 2.83, "누가 쓰나")):
        text(s, xx + 0.15, hy, ww, 0.4, t, size=13, bold=True, color=MUTED)
    for i, (c, what, who) in enumerate(rows):
        y = 2.1 + i * 0.78
        box(s, 0.6, y, 12.13, 0.68, fill=WHITE if i % 2 else SAND, line=LINE if i % 2 else None, radius=0.12, lw=0.5)
        text(s, 0.75, y, 2.8, 0.68, c, size=14, bold=True, color=RUST, font=MONO, anchor=MSO_ANCHOR.MIDDLE)
        text(s, 3.75, y, 6.0, 0.68, what, size=13.5, anchor=MSO_ANCHOR.MIDDLE)
        text(s, 10.05, y, 2.6, 0.68, who, size=13, color=MUTED, anchor=MSO_ANCHOR.MIDDLE)


def verification(d: Deck):
    s = d.content("검증", "무엇으로 믿는가",
                  "숫자는 모두 기록 파일에서 옮겼다. 인수 시험은 최종 7/7이지만 처음 두 번은 2/7이었고 원인을 찾지 못했다. "
                  "측정치는 판본 5.1 이전 엔진의 것이며 다시 재지 않았다.")
    stats = [
        ("11", "관문 모두 통과", "cargo xtask verify: 서식·린트·API 문서·정적·상태 기계·런타임·인수·프로토콜·서버·워커·프록시"),
        ("264", "시험 통과 · 실패 0", "같은 실행의 시험 결과 줄 합계"),
        ("68", "apibondrust 시험 통과", "ruvent 위의 실제 응용. 코드 변경 없이 빌드"),
        ("7/7", "실제 프로세스 인수", "최종 결과. 첫 두 실행은 2/7(아래)"),
    ]
    for i, (n, t, b) in enumerate(stats):
        x = 0.6 + i * 3.06
        box(s, x, 1.6, 2.88, 2.55, fill=WHITE, line=LINE, radius=0.06, lw=0.75)
        text(s, x + 0.25, 1.75, 2.5, 1.0, n, size=48, bold=True, color=RUST if i < 2 else TEAL, font=MONO)
        text(s, x + 0.25, 2.8, 2.5, 0.4, t, size=14.5, bold=True, color=INK_2)
        text(s, x + 0.25, 3.25, 2.45, 1.3, b, size=11.5, color=MUTED)
    box(s, 0.6, 4.45, 12.13, 1.0, fill=RUST_SOFT, radius=0.08)
    text(s, 0.85, 4.45, 11.7, 1.0, [
        {"runs": [("열린 문제  ", {"bold": True, "color": RUST}),
                  ("인수 시험의 첫 두 실행에서 스트리밍이 약 2.5초 만에 빈 본문으로 끝나고 그 뒤 같은 스택이 멈췄다. "
                   "v5 기준선과 이후 다섯 실행은 7/7이었고 재현 시험도 실패를 만들지 못했다. 원인은 아직 모른다.", {})]},
    ], size=13, anchor=MSO_ANCHOR.MIDDLE)
    text(s, 0.6, 5.75, 12.1, 0.85, [
        {"t": "기록: docs/reports/test-2026-11-26-worker-uuid-claim.txt · apibondrust/tests/reports/", "color": MUTED},
        {"t": "참고 측정(2026-11-23, 그 시점 엔진, 64 태스크 인프로세스 등록→착수→완료): 휘발 249,143회/초, 파일 strict 33,134회/초", "color": MUTED},
    ], size=12, space_after=4)


def constraints(d: Deck):
    s = d.content("알고 쓰기", "결정된 제약과 그 대가",
                  "이 제약들은 실수가 아니라 결정이다. 바꾸려면 제안서의 해당 결정부터 다시 연다. 전체 목록은 docs/constraints.md에 있다.")
    items = [
        ("최소 1회 실행", "lease 만료·완료 유실·비정상 종료는 재실행을 낳는다. 워커의 일은 멱등해야 한다."),
        ("서버 하나 = 장부 하나", "서버 안 복제·분할이 없다. 디스크를 잃으면 그 서버의 대기 이벤트도 잃는다."),
        ("발송은 그 순간만", "구독자가 없거나 끊겨 있으면 그대로 사라진다. 결과를 지킬지는 워커와 프로젝트가 정한다."),
        ("워커 uuid는 인증이 아니다", "다른 워커의 uuid를 아는 자격은 그 착수를 완료·반환할 수 있다. 권한은 메서드 단위뿐이다."),
        ("정책 없는 서버", "끝없이 실패하는 이벤트는 워커가 실패 횟수를 보고 끝내지 않으면 계속 돈다."),
        ("끊김 감지 약 30초", "조용한 끊김은 커널 keepalive로 안다. 앱 heartbeat는 없다."),
    ]
    for i, (t, b) in enumerate(items):
        x = 0.6 + (i % 3) * 4.13
        y = 1.6 + (i // 3) * 2.45
        card(s, x, y, 3.85, 2.2, t, b, accent=RUST if i in (0, 3) else INK_2, title_size=16, body_size=14)


def next_steps(d: Deck):
    s = d.blank(dark=True)
    text(s, 0.9, 0.75, 11, 0.4, "다음 걸음", size=13, color=RUST, bold=True)
    text(s, 0.9, 1.1, 11.5, 0.8, "어디서부터 읽고, 무엇을 돌려 볼까", size=30, color=WHITE, bold=True)
    docs = [
        ("overview", "하는 것·안 하는 것·보장"),
        ("architecture", "엔진·엔벨롭·저널·와이어"),
        ("usage", "서버·워커·프록시 쓰는 법"),
        ("constraints", "결정된 제약과 대가"),
        ("testing", "관문과 요구 ↔ 시험 지도"),
    ]
    for i, (n, desc) in enumerate(docs):
        x = 0.9 + i * 2.38
        badge(s, x, 2.3, i + 1, color=RUST, d=0.48, size=15)
        text(s, x, 2.95, 2.25, 0.4, f"docs/{n}.md", size=12.5, color=WHITE, bold=True, font=MONO)
        text(s, x, 3.35, 2.2, 0.7, desc, size=12.5, color=ON_INK)
    b = box(s, 0.9, 4.45, 11.53, 2.05, fill=INK_2, radius=0.04)
    label(b, [
        {"t": "# 전체 검증 관문", "color": ON_INK},
        {"t": "cargo xtask verify", "color": WHITE, "after": 8},
        {"t": "# 이벤트 서버 띄우기", "color": ON_INK},
        {"t": "cargo run --release -p ruvent-server -- crates/ruvent-server/server.example.json", "color": WHITE},
    ], size=13.5, font=MONO, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.MIDDLE, margin=0.35, space_after=2)
    text(s, 0.9, 6.75, 11.5, 0.4, "설계의 근거와 사용자 원문: docs/proposals/library-overhaul.md", size=12.5, color=ON_INK)
    s.notes_slide.notes_text_frame.text = (
        "문서는 overview → architecture → usage → constraints → testing 순서로 읽으면 된다. "
        "처음 손댈 때는 cargo xtask verify부터 돌려 관문이 모두 통과하는지 본다."
    )


def main() -> None:
    d = Deck()
    for build in (cover, one_sentence, why, big_picture, words, two_kinds, lifecycle, claim_rule,
                  request_reply, not_doing, envelope, engine, durability, guarantees, code_taste,
                  crates, verification, constraints, next_steps):
        build(d)
    d.prs.save(OUT)
    print(f"wrote {OUT} ({d.count} slides)")


if __name__ == "__main__":
    main()
