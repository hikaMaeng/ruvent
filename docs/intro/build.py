"""Builds docs/intro/ruvent-intro.pptx: ruvent for a developer who has never seen it.

    python docs/intro/build.py                         # docs/intro/ruvent-intro.pptx
    python docs/intro/build.py target/intro-draft.pptx # while the deck is open in PowerPoint
    powershell -NoProfile -ExecutionPolicy Bypass -File docs/proposals/deck/render.ps1 \
        -Deck docs/intro/ruvent-intro.pptx -Out target/intro-render

Writing and look rules are in docs/intro/README.md. In short: a title (noun
phrase) and a "핵심:" line per slide, noun-form endings (함 · 됨 · 없음), no
bold, no coined Korean names for code concepts (use the real name, gloss it
once), two semantic colours (rust = claimable, teal = dispatch), real artifacts
instead of stat callouts. Every number is copied from the file named next to it.
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
RUST = RGBColor(0xB5, 0x4A, 0x26)    # claimable
TEAL = RGBColor(0x23, 0x6E, 0x6C)    # dispatch
TINT_RUST = RGBColor(0xF7, 0xE7, 0xE0)
TINT_TEAL = RGBColor(0xE0, 0xEF, 0xEE)

W, H = 13.333, 7.5
L = 0.75          # left margin
CW = W - 2 * L    # content width
TOP = 1.75        # where content starts under title + 핵심


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


def bulletize(paragraph, indent: float = 0.25, level: int = 0) -> None:
    ppr = paragraph._p.get_or_add_pPr()
    left = indent * (level + 1)
    ppr.set("marL", str(int(Inches(left))))
    ppr.set("indent", str(-int(Inches(indent))))
    clr = etree.SubElement(ppr, qn("a:buClr"))
    etree.SubElement(clr, qn("a:srgbClr")).set("val", str(GRAY))
    etree.SubElement(ppr, qn("a:buFont")).set("typeface", "Arial")
    etree.SubElement(ppr, qn("a:buChar")).set("char", "–")


def fill_frame(tf, paras, size, color, align, font, after):
    """paras: str | dict(t, size, color, font, after, bullet, level, align). No bold anywhere."""
    tf.word_wrap = True
    tf.auto_size = MSO_AUTO_SIZE.NONE
    for i, item in enumerate(paras):
        spec = item if isinstance(item, dict) else {"t": item}
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = spec.get("align", align)
        p.space_after = Pt(spec.get("after", after))
        r = p.add_run()
        r.text = spec.get("t", "")
        r.font.size = Pt(spec.get("size", size))
        r.font.bold = False
        r.font.color.rgb = spec.get("color", color)
        set_font(r, spec.get("font", font))
        if spec.get("bullet"):
            bulletize(p, level=spec.get("level", 0))


def text(slide, x, y, w, h, paras, size=15, color=BLACK, align=PP_ALIGN.LEFT,
         anchor=MSO_ANCHOR.TOP, font=FONT, after=0):
    tb = slide.shapes.add_textbox(inch(x), inch(y), inch(w), inch(h))
    tf = tb.text_frame
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = anchor
    fill_frame(tf, [paras] if isinstance(paras, (str, dict)) else paras, size, color, align, font, after)
    return tb


def items(groups, sub_size=None):
    """[(head, [sub, ...]), ...] → numbered heads ("1.머리") with dash sub-items."""
    out = []
    for n, (head, subs) in enumerate(groups, 1):
        out.append({"t": f"{n}.{head}", "after": 3})
        for k, sub in enumerate(subs):
            spec = sub if isinstance(sub, dict) else {"t": sub}
            spec = {**spec, "bullet": True, "after": 10 if k == len(subs) - 1 else 2}
            if sub_size:
                spec.setdefault("size", sub_size)
            out.append(spec)
    return out


def code(slide, x, y, w, h, lines, size=12.5, anchor=MSO_ANCHOR.TOP):
    s = rect(slide, x, y, w, h, fill=CODE_BG)
    tf = s.text_frame
    tf.margin_left = tf.margin_right = inch(0.3)
    tf.margin_top = tf.margin_bottom = inch(0.22)
    tf.vertical_anchor = anchor
    paras = [{"t": ln or " ", "color": GRAY if ln.lstrip().startswith(("//", "#")) else BLACK} for ln in lines]
    fill_frame(tf, paras, size, BLACK, PP_ALIGN.LEFT, MONO, 2)
    return s


# ── shapes ──────────────────────────────────────────────────────────────────


def rect(slide, x, y, w, h, fill=None, line=None, lw=1.0, dash=False):
    s = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, inch(x), inch(y), inch(w), inch(h))
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
    fill_frame(tf, [paras] if isinstance(paras, (str, dict)) else paras, size, color, align, FONT, 0)
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
            text(slide, cx, cy, w - 0.15, 0.4, cell, size=12.5, color=(header_colors or {}).get(i, GRAY),
                 anchor=MSO_ANCHOR.BOTTOM)
            cx += w
        cy += 0.48
        hrule(slide, x, cy, sum(widths), color=BLACK, width=1)
    for row in rows:
        cx = x
        for cell, w in zip(row, widths):
            spec = cell if isinstance(cell, dict) else {"t": cell}
            text(slide, cx, cy, w - 0.2, row_h, spec, size=spec.get("size", size), anchor=MSO_ANCHOR.MIDDLE,
                 color=spec.get("color", BLACK), font=spec.get("font", FONT))
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

    def slide(self, title, core, notes):
        s = self.prs.slides.add_slide(self.prs.slide_layouts[6])
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = WHITE
        self.count += 1
        if title:
            text(s, L, 0.5, CW, 0.55, title, size=26)
        if core:
            text(s, L, 1.12, CW, 0.4, f"핵심: {core}", size=15, color=GRAY)
        if self.count > 1:
            text(s, W - L - 1, 7.0, 1, 0.25, str(self.count), size=10, color=GRAY, align=PP_ALIGN.RIGHT)
        s.notes_slide.notes_text_frame.text = notes
        return s


# ── slides ──────────────────────────────────────────────────────────────────


def cover(d: Deck):
    s = d.slide(None, None, "ruvent를 처음 보는 개발자용 소개. 연결 구조, 이벤트 두 종류, 착수 규칙, 보장과 제약 순서.")
    text(s, L, 2.3, CW, 1.0, "ruvent", size=54, font=MONO)
    text(s, L, 3.4, 11, 0.5, "Rust로 만든 이벤트 서버", size=24)
    text(s, L, 4.0, 11, 0.5, "작업은 워커 한 곳에 배정, 결과는 그 시점의 구독자에게 바로 전달", size=18, color=GRAY)
    hrule(s, L, 5.3, CW, color=BLACK, width=1)
    text(s, L, 5.45, CW, 0.9, [
        "처음 보는 개발자용 소개 · 2026-11-27",
        "기준 판본 5.1(wire v6, 저널 형식 7) · 저장소 F:\\dev\\ruvent",
    ], size=13, color=GRAY, after=4)


def why(d: Deck):
    s = d.slide("만든 이유",
                "PostgreSQL + PGMQ에 묶인 브로커 구조를 DB 없는 단독 서버로 교체",
                "원본은 ndx_business의 packages/agent. 부분별 대응표는 docs/origin.md.")
    colw = (CW - 0.6) / 2
    for i, (head, sub, rows) in enumerate([
        ("기존 packages/agent", "TypeScript · PostgreSQL + PGMQ", [
            "브로커 · 워커 서버 · PGMQ · 클라이언트가 PostgreSQL 하나를 공유",
            "명령은 PGMQ 큐, 관측은 event_store 로그로 분리",
            "브로커가 클라이언트별 커서를 저장하면서 로그를 읽어 전송",
            "브로커가 action → 큐 매핑표를 보고 기록을 큐에 복사",
        ]),
        ("ruvent", "Rust · 자체 저널", [
            "서버 한 대가 상태를 메모리에 보관, 상태 변경은 태스크 하나만 담당",
            "DB 없이 자체 저널 파일로 재시작 후 복구",
            "발송 이벤트는 저장 없이 등록 즉시 구독자에게 전달",
            "처리량과 가용성은 독립 서버 대수로 확보",
        ]),
    ]):
        x = L + i * (colw + 0.6)
        text(s, x, TOP, colw, 0.45, head, size=18)
        text(s, x, TOP + 0.45, colw, 0.35, sub, size=13, color=GRAY)
        hrule(s, x, TOP + 0.95, colw, color=BLACK, width=1)
        text(s, x, TOP + 1.15, colw, 3.6, [{"t": t, "bullet": True} for t in rows], size=15, after=12)


def big_picture(d: Deck):
    s = d.slide("연결 구조",
                "이벤트 서버에 직접 붙는 쪽은 프록시와 워커뿐, 일반 클라이언트는 프록시 경유",
                "이벤트 서버의 프로토콜은 TCP 위 wire v6 하나. HTTP 같은 클라이언트 프로토콜 처리는 프록시 몫. "
                "서버는 접속한 쪽이 프록시인지 워커인지 구분하지 않고, 권한도 메서드 단위로만 확인.")
    node(s, L, 2.15, 2.1, 0.95, ["클라이언트", {"t": "HTTP 등", "size": 12, "color": GRAY}])
    node(s, 3.65, 2.15, 2.6, 0.95, ["프록시", {"t": "프로젝트에서 직접 구현", "size": 12, "color": GRAY}])
    node(s, 3.65, 4.05, 2.6, 0.95, ["워커 서버", {"t": "ruvent-client 사용", "size": 12, "color": GRAY}])
    line(s, L + 2.12, 2.62, 3.6, 2.62)
    line(s, 6.27, 2.62, 7.55, 2.62)
    line(s, 6.27, 4.52, 7.55, 4.52)
    text(s, 6.3, 2.25, 1.2, 0.3, "wire v6", size=11, color=GRAY, align=PP_ALIGN.CENTER, font=MONO)
    text(s, 6.3, 4.15, 1.2, 0.3, "wire v6", size=11, color=GRAY, align=PP_ALIGN.CENTER, font=MONO)

    rect(s, 7.6, 1.7, 4.98, 3.75, line=BLACK, lw=1.5)
    text(s, 7.85, 1.85, 4.5, 0.35, "ruvent-server", size=14, font=MONO)
    node(s, 7.85, 2.35, 4.48, 0.8, [{"t": "착수용 큐", "color": RUST},
                                    {"t": "완료 보고 시 삭제 · 저널에 기록", "size": 12}],
         fill=TINT_RUST, line=None, align=PP_ALIGN.LEFT)
    node(s, 7.85, 3.3, 4.48, 0.8, [{"t": "발송", "color": TEAL},
                                   {"t": "등록 즉시 구독자에게 전달 후 삭제", "size": 12}],
         fill=TINT_TEAL, line=None, align=PP_ALIGN.LEFT)
    node(s, 7.85, 4.25, 4.48, 0.8, ["내장 워커", {"t": "같은 메서드를 소켓 없이 함수로 호출", "size": 12}],
         line=GRAY, dash=True, align=PP_ALIGN.LEFT)
    text(s, L, 5.85, CW, 0.9, [
        {"t": "서버의 연결 수 = 프록시 수 + 워커 서버 수, 클라이언트 수와 무관", "bullet": True},
        {"t": "ruvent-proxy는 골격만 제공(서버별 관찰 연결, 채널별 팬아웃). 인증 · 요청-응답 매칭 · 역압은 프로젝트 몫",
         "bullet": True},
    ], size=14, after=6)


def two_kinds(d: Deck):
    s = d.slide("이벤트 두 종류",
                "착수용 이벤트는 작업, 발송 이벤트는 결과 전달용",
                "엔벨롭 형식은 하나이고 flags 비트 0으로 종류를 구분. 착수용은 최소 1회 실행이라 워커 쪽 멱등 처리가 필요.")
    rows = [
        ("받는 쪽", "참여 워커 중 하나", "등록 시점의 채널 구독자 전부"),
        ("삭제 시점", "그 워커의 완료 보고", "구독자에게 전달한 직후"),
        ("저장", "저널에 기록, 재시작 후 복구", "저장 안 함"),
        ("놓친 경우", "lease(착수 유지 기한) 만료 후 회수 · 재배정", "끊겨 있던 구독자는 수신 불가, 재전송 없음"),
        ("실행 횟수", "최소 1회, 워커 쪽 멱등 처리 필요", "등록 시점 구독자에게 1회"),
        ("예", "API 중계, 계산, 다음 단계 작업", "결과, 스트리밍 조각, 알림"),
    ]
    table(s, L, TOP, [2.3, 4.95, 4.58], rows, header=["", "착수용 이벤트", "발송 이벤트"], size=15, row_h=0.68,
          header_colors={1: RUST, 2: TEAL})


def envelope(d: Deck):
    s = d.slide("엔벨롭 구조",
                "서버 동작에 쓰는 필드만 포함, 나머지 내용은 전부 payload 안",
                "고정 머리 38바이트 뒤에 필요할 때만 붙는 필드. 이름 사전 없음. "
                "레코드 종류(요청, 알림, 배정, 전달, 저널)별로 어떤 필드가 실리는지는 Layout에서 결정.")
    text(s, L, TOP, CW, 0.35, "고정 머리 38바이트(칸 폭은 바이트 수에 비례)", size=13, color=GRAY)
    x = L
    for name, n in (("id", 16), ("channel", 16), ("type", 2), ("flags", 2), ("fail", 2)):
        w = CW * n / 38
        node(s, x, TOP + 0.4, w, 0.7, [{"t": name, "font": MONO, "size": 12 if n == 16 else 9.5},
                                       {"t": f"{n}", "size": 10, "color": GRAY}], lw=0.75)
        x += w
    defs = [
        ("id", "UUIDv7(생성 시각순으로 정렬되는 UUID). 회수 · 재시작에도 불변"),
        ("channel", "이름 없는 UUIDv7. 보통 요청 하나에 채널 하나, 발송 이벤트는 채널 단위로 전달"),
        ("type", "u16 번호. 착수용 이벤트를 워커에 나눌 때만 사용, 번호의 의미는 프로젝트에서 결정"),
        ("flags", "비트 0 종류, 비트 1 not_before, 비트 2 워커 uuid 유무, 나머지 예약"),
        ("failures", "이벤트 원인의 실패 횟수(lease 만료, 실패 반환), 최대 65,535"),
        ("+ 작업 키", "(세대, 도착 순번). 착수용 이벤트의 대기 순서이자 워커 쪽 중복 실행 판별 기준"),
        ("+ 워커 uuid", "16바이트, 배정 메시지에만 포함. 값이 있으면 착수 상태"),
        ("+ payload", "길이 4바이트 + 본문. 서버는 해석 안 함"),
    ]
    rows = [({"t": k, "font": MONO, "size": 13.5, "color": RUST if "워커" in k else BLACK}, v) for k, v in defs]
    table(s, L, TOP + 1.3, [2.3, CW - 2.3], rows, size=13.5, row_h=0.47)


def lifecycle(d: Deck):
    s = d.slide("착수용 이벤트 처리 흐름",
                "등록 → 대기 → 배정 → 실행 → 완료, 실패 시 대기로 복귀",
                "배정은 서버 쪽에서 밀어 주는 방식. 워커는 Join으로 채널이나 타입을 지정해 참여 등록만 하고, 동시에 받을 개수(window)를 알림. "
                "서버는 빈자리가 있는 워커에 등록 순서대로 돌아가며 배정. 폴링 없음.")
    steps = [
        ("등록", "작업 키 부여"),
        ("대기", "(채널, 타입)별 대기열"),
        ("배정", "빈자리가 있는 워커에\n등록 순서대로 차례 배정"),
        ("실행", "lease 동안 실행,\n연장 가능"),
        ("완료", "삭제 후 후속 이벤트를\n같은 처리 단위에서 등록"),
    ]
    bw, gap = 2.0, 0.46
    y0 = TOP + 0.15
    for i, (name, desc) in enumerate(steps):
        x = L + i * (bw + gap)
        col = TEAL if name == "완료" else (RUST if name in ("배정", "실행") else BLACK)
        node(s, x, y0, bw, 0.8, {"t": name, "color": col, "size": 17}, line=col, lw=1.5)
        text(s, x, y0 + 0.92, bw, 0.8, desc, size=13, color=GRAY, align=PP_ALIGN.CENTER)
        if i < 4:
            line(s, x + bw + 0.04, y0 + 0.4, x + bw + gap - 0.04, y0 + 0.4)
    run_cx = L + 3 * (bw + gap) + bw / 2
    wait_cx = L + 1 * (bw + gap) + bw / 2
    line(s, run_cx, y0 + 1.75, run_cx, y0 + 2.35, color=RUST)
    node(s, 5.6, y0 + 2.35, 5.0, 1.0, [
        {"t": "반환: 워커 uuid 삭제, 실패로 반환하면 실패 횟수 +1", "size": 13.5},
        {"t": "lease 만료: 회수, 실패 횟수 +1", "size": 13.5},
    ], line=RUST, align=PP_ALIGN.LEFT)
    line(s, 5.6, y0 + 2.85, wait_cx, y0 + 2.85, color=RUST, head=False)
    line(s, wait_cx, y0 + 2.85, wait_cx, y0 + 1.75, color=RUST)
    text(s, L, 5.75, CW, 1.1, [
        {"t": "대기로 돌아간 이벤트도 id는 그대로, 다른 워커가 착수 가능", "bullet": True},
        {"t": "서버는 실패 횟수만 집계, 포기 시점은 핸들러가 Context::failures()로 판단", "bullet": True},
        {"t": "배정은 서버 쪽 푸시 방식. 워커는 Join으로 참여 등록만 하고 폴링 없음", "bullet": True},
    ], size=13.5, after=5)


def claim_rule(d: Deck):
    s = d.slide("착수 식별 규칙(판본 5.1)",
                "착수는 (이벤트 id, 워커 uuid) 한 쌍으로만 식별, 별도의 착수 토큰 없음",
                "엔벨롭에 워커 uuid가 있으면 착수 상태, 없으면 대기 상태. 보고는 두 값이 모두 맞을 때만 반영되고, "
                "어긋나면 stale 카운터만 올림.")

    def env(y, title, worker):
        text(s, L, y, 6, 0.32, title, size=13, color=GRAY)
        x = L
        for name, w in (("id", 1.1), ("channel", 1.2), ("type·flags·failures", 2.3)):
            node(s, x, y + 0.38, w, 0.6, {"t": name, "font": MONO, "size": 11}, lw=0.75)
            x += w
        if worker:
            node(s, x, y + 0.38, 1.4, 0.6, {"t": "worker uuid", "font": MONO, "size": 11, "color": WHITE},
                 fill=RUST, line=RUST)
        else:
            node(s, x, y + 0.38, 1.4, 0.6, {"t": "없음", "size": 11.5, "color": GRAY}, line=RULE, dash=True)

    env(TOP, "대기 상태", False)
    env(TOP + 1.2, "착수 상태", True)
    text(s, L, TOP + 2.55, 6.0, 1.3, [
        {"t": "워커 uuid는 Worker 인스턴스당 하나, SDK가 자동 입력", "bullet": True},
        {"t": "lease 기한은 엔벨롭 밖, 서버의 착수 기록에 보관", "bullet": True},
    ], size=13.5, after=6)
    x0 = 7.05
    text(s, x0, TOP, CW + L - x0, 4.2, items([
        ("착수 요청에 워커 uuid 필수", ["Join(참여 등록) · Claim(직접 착수) 모두 워커 uuid 포함", "빈 값(nil)이면 거절"]),
        ("보고는 두 값이 모두 맞을 때만 반영", ["완료 · 연장 · 반환 모두 이벤트 id와 워커 uuid 전송",
                                         "하나라도 다르면 stale로 집계만 하고 상태 변경 없음"]),
        ("맞는 실패 보고는 착수 해제", ["워커 uuid 삭제, 실패 횟수 +1", "이후 다른 워커가 착수 가능"]),
    ], sub_size=13.5), size=15.5)
    text(s, L, 5.85, CW, 0.9, [
        {"t": "알려진 제약", "color": GRAY, "size": 12.5, "after": 3},
        {"t": "같은 워커가 lease 만료 이벤트를 다시 배정받으면 이전 실행의 늦은 완료도 수용됨", "bullet": True},
        {"t": "워커 uuid는 식별용, 인증 수단 아님", "bullet": True},
    ], size=12.5, color=GRAY, after=2)


def request_reply(d: Deck):
    s = d.slide("요청-응답 흐름",
                "요청마다 새 채널을 하나 만들고 응답도 그 채널로 수신",
                "어느 요청의 응답인지는 채널로 구분. 프록시가 요청마다 새 채널을 만들어 먼저 구독하고 그 채널에 작업을 등록. "
                "워커는 타입으로 참여해 작업을 받고 같은 채널로 결과를 발송.")
    cols = [("클라이언트", 1.5), ("프록시", 4.0), ("이벤트 서버", 6.65), ("워커", 9.3), ("추론 API 서버", 11.8)]
    for name, cx in cols:
        node(s, cx - 1.0, TOP - 0.05, 2.0, 0.5, {"t": name, "size": 13})
        line(s, cx, TOP + 0.47, cx, 6.3, color=RULE, width=1, dash=True, head=False)
    xs = dict(cols)
    msgs = [
        ("클라이언트", "프록시", "API 요청", BLACK),
        ("프록시", "이벤트 서버", "새 채널 C 구독", BLACK),
        ("프록시", "이벤트 서버", "C에 작업 등록(타입 CALL)", RUST),
        ("이벤트 서버", "워커", "워커에 배정", RUST),
        ("워커", "추론 API 서버", "추론 서버로 중계", BLACK),
        ("워커", "이벤트 서버", "응답 조각을 C로 발송", TEAL),
        ("이벤트 서버", "프록시", "구독자에게 즉시 전달", TEAL),
        ("프록시", "클라이언트", "스트리밍 응답", TEAL),
        ("워커", "이벤트 서버", "완료 보고", RUST),
    ]
    for i, (a, b, lab, col) in enumerate(msgs):
        y = TOP + 0.85 + i * 0.42
        xa, xb = xs[a], xs[b]
        sgn = 1 if xb > xa else -1
        line(s, xa + 0.05 * sgn, y, xb - 0.05 * sgn, y, color=col, width=1.5)
        text(s, min(xa, xb), y - 0.3, abs(xb - xa), 0.27, f"{i + 1}. {lab}", size=11, color=col,
             align=PP_ALIGN.CENTER)
    text(s, L, 6.45, CW, 0.5,
         "6~8은 스트리밍 동안 반복. 실제 적용 사례는 apibondrust(OpenAI · Anthropic 요청을 여러 추론 서버로 분산하는 중계기를 ruvent 위에 재구현)",
         size=12.5, color=GRAY)


def policy(d: Deck):
    s = d.slide("서버에 없는 기능과 담당",
                "정책은 서버 밖에 두고 워커와 프록시가 담당",
                "서버에 정책을 넣지 않는 것이 이 프로젝트의 핵심 결정. 근거는 docs/proposals/library-overhaul.md 9.15.")
    rows = [
        ("재시도 상한, dead letter 격리", "핸들러가 Context::failures()를 확인해 Ok(())로 종료, 필요하면 실패 결과를 후속 이벤트로 발행"),
        ("순서 보장", "앞 작업을 끝낸 워커가 다음 작업을 후속 이벤트로 등록(완료 보고와 같은 처리 단위)"),
        ("만료", "워커가 payload나 failures 값을 보고 종료 판단"),
        ("흐름 제어", "생산자 쪽 미확인 바이트 상한(연결당 64 MiB), 상한 도달 시 send 대기. 서버는 등록 거절 안 함"),
        ("발송 재전송", "없음. 결과 보존이 필요하면 워커와 프로젝트에서 설계"),
        ("채널별 권한", "없음. 권한은 메서드 단위, 범위 분리가 필요하면 서버 분리"),
        ("payload 해석", "없음. 코어는 JSON 형식을 모름"),
    ]
    table(s, L, TOP, [3.0, CW - 3.0], rows, header=["서버에 없는 기능", "담당과 방법"], size=14, row_h=0.64)


def engine(d: Deck):
    s = d.slide("엔진 내부 구조",
                "상태를 고치는 태스크는 하나뿐(single writer), 락 없음",
                "태스크 하나가 상태를 소유하고 나머지는 메시지로 요청. State는 시각을 입력으로 받는 순수 상태 기계라 "
                "소켓과 시계 없이 시험 가능. 부수 효과는 Effects로 넘겨 각 스레드나 태스크가 처리.")
    node(s, L, 2.55, 2.0, 1.2, ["연결", {"t": "생산 · 작업 · 관찰", "size": 11.5, "color": GRAY}])
    line(s, L + 2.02, 3.15, 3.25, 3.15)
    node(s, 3.3, 2.7, 1.9, 0.9, [{"t": "inbox", "font": MONO}, {"t": "mpsc 채널", "size": 11.5, "color": GRAY}])
    line(s, 5.22, 3.15, 5.75, 3.15)
    node(s, 5.8, 2.3, 2.5, 1.7, [{"t": "State", "font": MONO, "size": 18},
                                 {"t": "순수 상태 기계", "size": 12.5},
                                 {"t": "태스크 · 소켓 · 시계 없음", "size": 11.5, "color": GRAY}], lw=2)
    node(s, 5.8, 4.75, 2.5, 0.75, ["타이밍 휠", {"t": "가장 이른 기한에 깨움", "size": 11.5, "color": GRAY}])
    line(s, 7.05, 4.73, 7.05, 4.05)
    effects = ["연결별 송신 큐", "로그 기록", "본문 읽기", "체크포인트 기록", "세그먼트 정리(이주 · 삭제)", "스냅샷 수집"]
    text(s, 9.3, TOP - 0.15, 3.3, 0.3, "Effects(부수 효과 처리)", size=12, color=GRAY)
    for k, e in enumerate(effects):
        y = 1.95 + k * 0.62
        node(s, 9.3, y, 3.28, 0.5, {"t": e, "size": 12.5}, line=RULE)
        line(s, 8.33, 3.15, 9.27, y + 0.25, color=GRAY, width=0.75)
    text(s, L, 5.95, CW, 0.9, [
        {"t": "서버는 어느 연결의 송신도 기다리지 않음. 느린 연결은 자기 송신 큐가 차면 끊김", "bullet": True},
        {"t": "로그 · 체크포인트 기록이나 본문 읽기에 실패하면 엔진이 사유를 남기고 정지", "bullet": True},
    ], size=14, after=6)


def journal(d: Deck):
    s = d.slide("저널 파일 구조",
                "처리가 끝나지 않은 착수용 이벤트의 변경분만 기록, 발송 이벤트는 기록 안 함",
                "세그먼트 파일과 체크포인트 하나로 재시작 후 상태를 복구.")
    code(s, L, TOP, 5.5, 2.3, [
        "data/journal/",
        "  lock              # 로그를 연 엔진 하나만 사용",
        "  0000000001.seg",
        "  0000000002.seg",
        "  checkpoint.ckpt   # 마지막으로 완성된 체크포인트",
    ], size=12.5)
    text(s, L, TOP + 2.55, 5.5, 1.8, [
        {"t": "세그먼트 파일 안의 영역 3가지", "after": 6},
        {"t": "로그: 레코드 프레임, 본문은 등록 레코드 안에 포함", "bullet": True},
        {"t": "장기 보관 영역: 다른 세그먼트에서 옮겨 온 본문", "bullet": True},
        {"t": "본문 영역: 디스크로 받아 둔 후속 이벤트 본문", "bullet": True},
    ], size=13.5, after=3)
    rows = [
        ("Strict", "등록 · 완료 · 삭제 응답은 fsync 이후 전송, 착수는 기다리지 않음"),
        ("체크포인트", "처리 묶음 사이사이에 상태를 조금씩 나눠 기록, 서비스 중단 없음"),
        ("복구", "체크포인트 + 이후 로그를 순서대로 재생. 착수 중이던 이벤트는 lease · 워커 uuid까지 복원, 실패 횟수 증가 없음"),
        ("손상", "마지막 세그먼트의 잘린 끝부분만 정리, 중간 손상은 정지 후 보고"),
        ("이전 형식", "형식 6 이하는 변환 없이 거부, 원본 바이트 보존"),
    ]
    table(s, 6.75, TOP, [1.6, CW + L - 6.75 - 1.6], rows, size=13, row_h=0.9)


def guarantees(d: Deck):
    s = d.slide("서버 보장 사항(G1~G8)",
                "보장 8가지, 항목마다 대응하는 시험 존재",
                "보장과 시험의 대응표는 docs/testing.md의 '보장 ↔ 테스트' 표.")
    rows = [
        ("G1", "대기 중인 착수용 이벤트에 동시에 착수하면 성공은 정확히 하나"),
        ("G2", "완료 · 실패 보고는 이벤트 id와 워커 uuid가 맞을 때만 반영. 경합에서 진 쪽의 보고와 완료 재전송은 후속 이벤트까지 무효"),
        ("G3", "작업 키는 등록 건마다 부여, 회수 · 재시작 후에도 유지"),
        ("G4", "대기 · 착수 중인 착수용 이벤트는 저널 기록 시점 기준으로 재시작 후 복구"),
        ("G5", "발송 이벤트는 등록 처리 시점의 구독자에게 전달 후 삭제, 버퍼가 가득 찬 연결은 끊김"),
        ("G6", "서버는 어느 연결의 송신도 기다리지 않음"),
        ("G7", "삭제는 대기 중일 때만 성공, 결과 통지는 정확히 1건"),
        ("G8", "착수용 큐에 있는 id로는 새 이벤트 등록 불가"),
    ]
    table(s, L, TOP, [0.8, CW - 0.8], [({"t": g, "font": MONO, "color": GRAY}, t) for g, t in rows],
          size=14, row_h=0.6)


def code_taste(d: Deck):
    s = d.slide("워커 코드 예",
                "핸들러 하나만 작성, 착수 · 연장 · 보고는 SDK가 처리",
                "Service::typed()는 채널과 상관없이 지정한 타입의 작업을 수신. 핸들러는 Ok 또는 Err(Retry)만 반환. "
                "link만 바꾸면 같은 코드로 프로세스 내부 엔진에도, TCP 서버에도 연결.")
    code(s, L, TOP, 8.0, 4.25, [
        "const CREATE: u16 = 10;   // 타입 번호는 프로젝트에서 지정",
        "const CREATED: u16 = 100;",
        "",
        "let worker = Worker::builder(link, &servers)",
        "    // 채널 무관, 타입 CREATE 수신. 동시 실행 최대 16건",
        "    .service(Service::typed().slots(16).on(CREATE, |cx| async move {",
        "        if downstream_is_down() {",
        "            // 이벤트 원인 아님 → 5초 뒤 반환, 실패 횟수 유지",
        "            return Err(Retry::later(Duration::from_secs(5)));",
        "        }",
        "        // 같은 채널로 결과 발송, 완료 보고에 후속 이벤트로 포함",
        "        cx.emit_dispatch(cx.channel(), CREATED, cx.payload().clone());",
        "        Ok(())   // 완료 보고(이벤트 id + 이 워커의 uuid)",
        "    }))",
        "    .start();",
        "",
        "worker.shutdown().await;   // 수신 중단 → 유예 → 남은 실행은 실패 아님으로 반환",
    ], size=11.5)
    text(s, 9.15, TOP, CW + L - 9.15, 4.5, items([
        ("SDK가 처리", ["착수 요청, lease 연장", "워커 uuid 입력", "완료 · 반환 보고", "재접속(지수 백오프 + full jitter)"]),
        ("핸들러가 결정", ["중단 시점(Context::failures())", "결과를 보낼 채널"]),
    ], sub_size=13.5), size=15)


def run_server(d: Deck):
    s = d.slide("서버 실행과 설정",
                "JSON 설정 파일 하나로 실행, 필수 항목은 listen과 credentials",
                "나머지 항목은 기본값 사용. 권한은 자격 증명마다 메서드 목록으로 부여. 전체 키와 기본값은 crates/ruvent-server/README.md.")
    code(s, L, TOP, 7.3, 3.75, [
        "// crates/ruvent-server/server.example.json 발췌",
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
    code(s, L, TOP + 3.9, 7.3, 0.5, ["cargo run --release -p ruvent-server -- crates/ruvent-server/server.example.json"],
         size=10.5, anchor=MSO_ANCHOR.MIDDLE)
    x = 8.45
    text(s, x, TOP, CW + L - x, 4.8, items([
        ("메서드 = 권한 단위", ["착수용 등록 · 발송 등록 · 착수(claim) · 완료(complete) · 삭제 · 구독 · 구독 취소의 7개",
                            "운영용 operate는 별도"]),
        ("메서드 하나에 호출 여러 개", ["claim → Join · Claim", "complete → Complete · Extend · Release"]),
        ("종료", ["Ctrl-C 시 drain 후 체크포인트 기록", "엔진이 스스로 멈추면 실패 코드로 종료"]),
    ], sub_size=13), size=14.5)


def crates(d: Deck):
    s = d.slide("크레이트 구성",
                "코어는 ruvent, 서버 · SDK · 프록시 골격이 그 위에 위치",
                "검증은 xtask 하나로 실행.")
    rows = [
        ("ruvent", "엔벨롭 · 엔진 · 저널 · 스냅샷, 메서드 정의, 프로세스 내부 연결", "전체"),
        ("ruvent-protocol", "바이너리 wire v6(핸드셰이크, 호출 · 응답 · 알림 · 전달 레코드)", "서버 · SDK 내부"),
        ("ruvent-server", "이벤트 서버 실행 파일, JSON 설정", "운영"),
        ("ruvent-client", "원격 연결 SDK, 워커 프레임워크(Worker · Service · 재시도)", "워커 서버, 프록시"),
        ("ruvent-proxy", "프록시 골격(서버별 관찰 연결, 채널별 팬아웃)", "프록시 구현 측"),
        ("xtask", "검증 명령 cargo xtask verify", "개발자"),
    ]
    table(s, L, TOP, [2.9, 6.6, CW - 9.5],
          [({"t": c, "font": MONO, "size": 13.5}, a, {"t": b, "color": GRAY}) for c, a, b in rows],
          header=["크레이트", "역할", "사용처"], size=14, row_h=0.68)


def verification(d: Deck):
    s = d.slide("검증 결과",
                "최근 cargo xtask verify 실행에서 검사 11단계 모두 통과",
                "왼쪽 줄은 docs/reports/test-2026-11-26-worker-uuid-claim.txt에서 그대로 옮긴 것. "
                "시험 수 264는 같은 파일의 test result 줄 합계.")
    code(s, L, TOP, 6.6, 3.45, [
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
    text(s, x, TOP, CW + L - x, 4.0, items([
        ("시험 264개 통과, 실패 0(같은 실행)", []),
        ("apibondrust(ruvent 기반 실제 응용)", ["코드 수정 없이 빌드, 시험 68개 통과", "실제 프로세스 인수 시험 최종 7/7"]),
        ("미해결 문제", [
            "인수 시험 첫 2회는 2/7. 스트리밍이 약 2.5초 만에 빈 본문으로 끝나고 이후 같은 스택이 응답 불가",
            "v5 기준 실행과 이후 5회는 7/7, 재현 시험으로도 실패 재현 못함. 원인 미확인",
        ]),
    ], sub_size=13), size=14.5)
    text(s, L, TOP + 3.65, CW, 0.5,
         "참고 측정(2026-11-23, 판본 5.1 이전 엔진, 64 태스크 · 프로세스 내부 · 등록→착수→완료 반복): 휘발 모드 249,143회/초, 파일 strict 33,134회/초. 이후 재측정 안 함",
         size=11.5, color=GRAY)


def constraints(d: Deck):
    s = d.slide("알려진 제약",
                "설계상 선택이며, 바꾸려면 제안서의 해당 결정부터 재검토",
                "전체 목록은 docs/constraints.md.")
    rows = [
        ("착수용 이벤트 최소 1회 실행", "lease 만료 · 완료 유실 · 비정상 종료 시 재실행 발생, 워커 작업은 멱등 필요"),
        ("서버 1대 = 상태 1벌", "서버 내부 복제 · 분할 없음, 디스크 손실 시 그 서버의 대기 이벤트도 손실"),
        ("발송은 등록 시점 구독자에게만", "구독자가 없거나 끊긴 상태면 유실"),
        ("워커 uuid는 인증 수단 아님", "다른 워커의 uuid를 아는 자격 증명이면 그 착수를 완료 · 반환 가능"),
        ("서버 쪽 정책 없음", "계속 실패하는 이벤트는 워커가 끝내지 않는 한 무한 반복"),
        ("앱 수준 heartbeat 없음", "응답 없는 연결은 커널 keepalive로 감지, 약 30초 소요"),
        ("메타데이터 메모리 상주", "관리 메모리가 대기 이벤트 수에 비례해 증가"),
    ]
    table(s, L, TOP, [4.0, CW - 4.0], rows, header=["제약", "영향"], size=14, row_h=0.64)


def first_day(d: Deck):
    s = d.slide("처음 할 일", None, "검증부터 돌려 환경을 확인한 뒤 overview 문서부터 읽기.")
    steps = [
        ("검증 실행", "cargo xtask verify", "11단계 모두 ok인지 확인, 마지막 줄이 verify status=ok가 아니면 환경부터 점검"),
        ("문서 읽기", "docs/overview.md → architecture.md → usage.md → constraints.md",
         "기능 범위 → 엔진 · 엔벨롭 · 저널 구조 → 사용법 → 제약 순서"),
        ("요구사항과 시험 대조", "docs/testing.md", "바꿀 동작에 걸린 시험부터 확인"),
    ]
    y = 1.5
    for i, (head, cmd, body) in enumerate(steps):
        text(s, L, y, CW, 0.42, f"{i + 1}.{head}", size=18)
        text(s, L + 0.3, y + 0.5, CW - 0.3, 0.35, cmd, size=13.5, font=MONO)
        text(s, L + 0.3, y + 0.92, CW - 0.3, 0.4, body, size=13.5, color=GRAY)
        y += 1.6
        hrule(s, L, y - 0.2, CW)
    text(s, L, 6.45, CW, 0.4, "설계 근거와 원문 요구사항: docs/proposals/library-overhaul.md", size=12.5, color=GRAY)


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
