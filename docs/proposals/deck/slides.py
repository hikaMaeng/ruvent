"""Slide content for the redesign deck. Each function draws one slide."""

# Names below (text, box, arrow, INK, ...) are injected by build.py via use().
# ruff: noqa: F821


def use(helpers):
    globals().update(helpers)


def I(value):
    return Inches(value)


FOOT = "ruvent · 이벤트 서버 재설계 제안 · 승인 대기"


def s01_title(prs, h):
    use(h)
    slide = slide_base(prs, "", dark=True)
    text(slide, MARGIN, I(1.6), I(8), I(0.4), "ruvent · 승인 요청", size=16, color=AMBER, bold=True)
    text(slide, MARGIN, I(2.1), I(8.2), I(1.6), "이벤트 서버\n재설계 제안", size=48, bold=True, color=PAPER, spacing=0)
    text(slide, MARGIN, I(4.05), I(7.6), I(1.2), ["역할 없는 이벤트 서버 · 프록시로 스케일아웃", "즉시 반환하는 워커 · 스케줄러 없는 착수 회수"], size=18, color=LINE)
    text(slide, MARGIN, I(6.55), I(9), I(0.3), "기준 커밋 f5a1439 · 외부 리뷰(Codex) 2회 반영 · 2026-11-22", size=12, color=LINE)
    # Motif: the target topology in miniature.
    for row in range(5):
        box(slide, I(9.0), I(1.7 + row * 0.62), I(0.42), I(0.42), "", fill=INK_2, shape=MSO_SHAPE.OVAL)
    for row in range(2):
        box(slide, I(10.0), I(2.15 + row * 1.25), I(1.0), I(0.62), "프록시", fill=AMBER, color=INK, size=11, bold=True)
        arrow(slide, I(11.0), I(2.46 + row * 1.25), I(11.55), I(3.35), color=LINE)
    box(slide, I(11.55), I(2.85), I(1.25), I(1.0), "이벤트\n서버", fill=PAPER, color=INK, size=12, bold=True)
    box(slide, I(11.6), I(4.6), I(1.15), I(0.6), "워커 서버", fill=TEAL, color=PAPER, size=11, bold=True)
    arrow(slide, I(12.18), I(4.6), I(12.18), I(3.85), color=LINE)


def s02_summary(prs, h):
    use(h)
    slide = slide_base(prs, "한 장 요약: 무엇을, 왜, 무엇을 승인하나", kicker="요약")
    cards = [
        ("지금의 한계", RED_SOFT, RED, ["워커가 긴 일을 붙잡아 풀이 멈출 수 있다", "착수에 기한이 없어 원격 워커가 죽으면 일이 묶인다", "모든 접속이 한 프로세스에 붙어 연결 수가 한계가 된다"]),
        ("바꿀 구조", TEAL_SOFT, TEAL, ["이벤트 서버는 역할 없이 종합 API만 제공", "프로토콜별 프록시 바이너리가 클라이언트를 받아낸다", "긴 일은 작업 레코드로 넘기고 워커는 즉시 반환"]),
        ("승인할 것", AMBER_SOFT, AMBER, ["보장 계약 G1~G7", "단계 계획 P0~P6 (측정부터)", "미결정 Q1~Q8의 방향"]),
    ]
    width = (W - 2 * MARGIN - I(0.6)) / 3
    for index, (title, fill, accent, lines) in enumerate(cards):
        x = MARGIN + index * (width + I(0.3))
        box(slide, x, I(1.85), width, I(4.6), "", fill=fill)
        text(slide, x + I(0.3), I(2.1), width - I(0.6), I(0.5), title, size=22, bold=True, color=accent)
        text(slide, x + I(0.3), I(2.85), width - I(0.6), I(3.4), [(line, {"bullet": True, "after": 12}) for line in lines], size=16)
    footer(slide, FOOT)


def s03_now(prs, h):
    use(h)
    slide = slide_base(prs, "지금 구조: 모든 것이 한 프로세스 안에", kicker="현재 (f5a1439)")
    box(slide, I(0.6), I(1.75), I(8.6), I(4.95), "", fill=MIST, line=LINE)
    text(slide, I(0.85), I(1.9), I(4), I(0.3), "한 프로세스", size=13, bold=True, color=MUTED)
    box(slide, I(3.6), I(2.55), I(2.6), I(1.5), "엔진", fill=INK, color=PAPER, size=18, bold=True, sub=["단일 작성자", "큐 · 실행 · 로그"])
    box(slide, I(0.9), I(2.4), I(2.1), I(0.62), "워커 lane A", fill=PAPER, line=LINE, size=13)
    box(slide, I(0.9), I(3.2), I(2.1), I(0.62), "워커 lane B", fill=PAPER, line=LINE, size=13)
    arrow(slide, I(3.0), I(2.71), I(3.6), I(3.0), both=True)
    arrow(slide, I(3.0), I(3.51), I(3.6), I(3.6), both=True)
    box(slide, I(6.8), I(2.75), I(2.1), I(1.1), "게이트웨이", fill=PAPER, line=LINE, size=14, sub="인증·정책 훅")
    arrow(slide, I(6.2), I(3.3), I(6.8), I(3.3), both=True)
    box(slide, I(3.6), I(4.75), I(2.6), I(0.7), "파일 저널 (순변화)", fill=PAPER, line=LINE, size=13)
    arrow(slide, I(4.9), I(4.05), I(4.9), I(4.75))
    for row in range(4):
        box(slide, I(10.4), I(2.0 + row * 0.75), I(0.5), I(0.5), "", fill=INK_2, shape=MSO_SHAPE.OVAL)
    text(slide, I(11.05), I(2.6), I(1.8), I(0.6), "클라이언트\n전부 직접 접속", size=12, color=MUTED)
    arrow(slide, I(10.4), I(3.3), I(8.9), I(3.3), dashed=True)
    text(slide, I(9.2), I(3.45), I(1.3), I(0.5), "전송 구현 없음", size=11, color=MUTED)
    marks = [(I(0.9), I(4.05), "D1 허가를 끝까지 쥠 · 기한 600초"), (I(0.9), I(4.55), "D2 넘긴 작업의 진행이 버려짐"), (I(6.5), I(4.2), "D7 잠금 중 구독자 깨우기"), (I(6.5), I(4.7), "D3 착수에 기한 없음"), (I(6.5), I(5.2), "D8 엔진이 멈춰도 프로세스는 산다")]
    for x, y, label in marks:
        box(slide, x, y, I(2.6) if x < I(3) else I(2.55), I(0.4), label, fill=RED_SOFT, color=RED, size=10, bold=True, align=PP_ALIGN.LEFT)
    text(slide, I(9.6), I(5.4), I(3.2), I(1.2), ["측정(91042f3): 왕복 15만/초", "3단 연쇄 3만/초 · Strict 왕복 4만/초"], size=12, color=MUTED)
    footer(slide, FOOT)


def s04_diagnosis(prs, h):
    use(h)
    slide = slide_base(prs, "진단: 목표와 어긋나는 여섯 지점", kicker="왜 바꾸나")
    cells = [
        ("D1", "워커가 긴 일을 붙잡는다", "허가를 완료 응답까지 보유, 기한 600초. 그 서비스 풀이 멈춘다"),
        ("D2·D3", "넘길 길도, 회수할 길도 없다", "넘긴 Context의 진행은 소리 없이 버려지고, 착수는 기한 없이 묶인다"),
        ("D4·D5", "내장 워커만 다른 길을 쓴다", "dequeue·begin·finish는 외부 프로토콜로 표현할 수 없다"),
        ("D6", "사용자 훅이 게이트웨이에", "커스터마이즈 없는 바이너리 프록시와 맞지 않는다"),
        ("D7", "발송 비용이 엔진 경로에", "게시 중 잠금을 쥐고 채널 구독자를 이벤트마다 깨운다"),
        ("D8", "장애가 밖으로 드러나지 않는다", "폴링 없이 기다릴 수단이 없고 shutdown은 오류를 버린다"),
    ]
    width = (W - 2 * MARGIN - I(0.6)) / 3
    for index, (tag, title, body) in enumerate(cells):
        col, row = index % 3, index // 3
        x = MARGIN + col * (width + I(0.3))
        y = I(1.8) + row * I(2.45)
        box(slide, x, y, width, I(2.2), "", fill=MIST)
        chip(slide, x + I(0.25), y + I(0.25), tag, fill=RED_SOFT, color=RED)
        text(slide, x + I(0.25), y + I(0.75), width - I(0.5), I(0.5), title, size=17, bold=True)
        text(slide, x + I(0.25), y + I(1.3), width - I(0.5), I(0.85), body, size=13, color=MUTED)
    text(slide, MARGIN, I(6.75), I(12), I(0.3), "그 밖: D9 핫패스 무게(측정 후 채택) · D10 Events::Custom이 서버 안에서 사용자 코드 실행", size=12, color=MUTED)


def s05_principles(prs, h):
    use(h)
    slide = slide_base(prs, "판정 기준은 사용자 원문이다", kicker="원칙")
    quotes = [
        "모든 워커는 재빨리 워커풀로 돌아와야해. 따라서 장기실행을 이벤트용 워커가 들고 있을 수는 없어.",
        "착수이벤트의 타임아웃은 일반적으로 스케줄러가 필요없어. … 이벤트 등록시점에 … 일괄 미착수 상태로 돌리기만 하면 되기 때문이야.",
        "이벤트서버는 종합적인 API를 제공하며 딱히 워커인지 프록시인지 클라이언트인지 관심없다.",
        "프록시서버는 바이너리로 배포되며 프록시서버용 설정은 JSON으로 한다.",
        "매우 섬세하게 사이사이 사소한 로직도 귀찮다고 가장 안전하고 무거운 구현을 하면 안된다.",
        "이벤트는 반드시 하나의 워커가 가져간다 … 하지만 인메모리만으로 운영되면 안된다.",
    ]
    width = (W - 2 * MARGIN - I(0.3)) / 2
    for index, quote in enumerate(quotes):
        col, row = index % 2, index // 2
        x = MARGIN + col * (width + I(0.3))
        y = I(1.75) + row * I(1.65)
        box(slide, x, y, width, I(1.45), "", fill=AMBER_SOFT)
        text(slide, x + I(0.2), y + I(0.05), I(0.5), I(0.6), "“", size=40, bold=True, color=AMBER)
        text(slide, x + I(0.75), y + I(0.25), width - I(1.0), I(1.1), quote, size=14, anchor=MSO_ANCHOR.MIDDLE)
    footer(slide, "원문 전체: docs/proposals/event-server-redesign.md 2장")


def s06_target(prs, h):
    use(h)
    slide = slide_base(prs, "목표 구조: 프록시가 받고, 이벤트 서버는 서버들만 접대한다", kicker="목표")
    for row in range(7):
        box(slide, I(0.7), I(1.85 + row * 0.66), I(0.45), I(0.45), "", fill=INK_2, shape=MSO_SHAPE.OVAL)
    text(slide, I(0.55), I(6.6), I(1.8), I(0.3), "일반 클라이언트", size=11, color=MUTED)
    proxies = [("WebSocket 프록시", 2.0), ("HTTP·SSE 프록시", 3.55), ("gRPC 프록시 (예정)", 5.1)]
    for label, y in proxies:
        box(slide, I(2.2), I(y), I(2.3), I(0.95), label, fill=AMBER, size=13, bold=True, sub="바이너리 + JSON")
        for row in range(2):
            arrow(slide, I(1.15), I(y + 0.1 + row * 0.7), I(2.2), I(y + 0.47), color=LINE, width=1.25)
        arrow(slide, I(4.5), I(y + 0.47), I(5.6), I(3.95), color=INK_2)
    box(slide, I(5.6), I(2.35), I(3.0), I(3.2), "", fill=INK)
    text(slide, I(5.85), I(2.55), I(2.6), I(0.4), "이벤트 서버", size=18, bold=True, color=PAPER)
    text(slide, I(5.85), I(3.05), I(2.6), I(1.3), ["종합 API", "단일 작성자", "순변화 저널", "사용자 코드 없음"], size=12, color=LINE, spacing=2)
    box(slide, I(5.85), I(4.5), I(2.5), I(0.8), "내장 워커", fill=INK_2, color=PAPER, size=12, line=AMBER, sub="같은 API를 함수로")
    servers = [("워커 서버 A", 2.3), ("워커 서버 B", 4.1)]
    for label, y in servers:
        box(slide, I(10.0), I(y), I(2.6), I(1.2), label, fill=TEAL, color=PAPER, size=14, bold=True, sub=["워커풀 여러 개", "작업 실행기"])
        arrow(slide, I(10.0), I(y + 0.6), I(8.6), I(3.95), color=INK_2, both=True)
    box(slide, I(5.4), I(6.0), I(7.2), I(0.6), "이벤트 서버의 연결 수 = 프록시 수 + 워커 서버 수 (사용자 수와 무관)", fill=TEAL_SOFT, color=TEAL, size=13, bold=True)
    footer(slide, FOOT)


def s07_api(prs, h):
    use(h)
    slide = slide_base(prs, "역할 없는 서버: 누구든 같은 API를 부른다", kicker="종합 API")
    callers = [("프록시 (사용자 대신)", AMBER, 1.95), ("워커 서버", TEAL, 3.35), ("내장 워커 (함수 호출)", INK_2, 4.75)]
    for label, fill, y in callers:
        box(slide, MARGIN, I(y), I(2.7), I(0.95), label, fill=fill, color=PAPER if fill != AMBER else INK, size=13, bold=True)
        arrow(slide, I(3.3), I(y + 0.47), I(4.3), I(y + 0.47))
    box(slide, I(4.3), I(1.85), I(1.0), I(4.0), "계약\n(trait)", fill=MIST, line=LINE, size=12, bold=True)
    text(slide, I(4.3), I(5.95), I(3.3), I(0.6), ["로컬 구현: 함수 호출", "원격 구현: 소켓"], size=11, color=MUTED)
    rows = [["메서드", "하는 일"],
            ["register", "이벤트 등록 → 판정(신규·진행 중 중복·완료 결과·충돌)"],
            ["receive · read", "채널 스트림 · 범위 읽기, 뒤처지면 gap"],
            ["claim", "대기 레코드를 여유분만큼 착수(번호·기한)"],
            ["complete", "번호가 현재면 결과 반영 + 이벤트 함께 등록"],
            ["extend · register_in", "기한 연장 · 착수 맥락의 중간 등록"],
            ["interest · reactions", "관심 채널 0↔1 전이 · 소유 범위 리액션"],
            ["flush", "지금까지의 변화가 디스크에 닿으면 응답"]]
    table(slide, I(5.7), I(1.85), I(7.0), rows, [1.6, 4.4], size=12, row_h=0.5)
    footer(slide, "권한은 역할이 아니라 서비스 자격과 위임 신원으로 판정")


def s08_states(prs, h):
    use(h)
    slide = slide_base(prs, "레코드 하나, 상태 넷: 대기 → 착수 → 완료", kicker="상태 전이")
    wait = box(slide, I(0.8), I(2.4), I(2.2), I(1.0), "대기", fill=MIST, line=LINE, size=18, bold=True)
    claim = box(slide, I(4.3), I(2.4), I(2.9), I(1.0), "착수", fill=INK, color=PAPER, size=18, bold=True, sub="번호 n · 기한 t · 소유 접속 c")
    done = box(slide, I(8.6), I(2.4), I(2.2), I(1.0), "완료 기록", fill=TEAL_SOFT, color=TEAL, size=16, bold=True, sub="창 안에서만")
    gone = box(slide, I(11.4), I(2.55), I(1.3), I(0.7), "제거", fill=PAPER, line=LINE, size=14)
    delay = box(slide, I(4.55), I(4.9), I(2.4), I(0.9), "지연", fill=AMBER_SOFT, color=INK, size=16, bold=True, sub="재시도 대기 (활성)")
    dead = box(slide, I(0.8), I(4.9), I(2.2), I(0.9), "dead letter", fill=RED_SOFT, color=RED, size=15, bold=True)
    arrow(slide, I(3.0), I(2.75), I(4.3), I(2.75))
    text(slide, I(3.05), I(2.35), I(1.3), I(0.3), "착수 · 시도 +1", size=11, color=MUTED)
    arrow(slide, I(4.3), I(3.15), I(3.0), I(3.15), color=RED)
    text(slide, I(3.0), I(3.25), I(1.4), I(0.6), "기한 지남\n접속 끊김", size=11, color=RED)
    arrow(slide, I(7.2), I(2.9), I(8.6), I(2.9), color=TEAL)
    text(slide, I(7.25), I(2.5), I(1.4), I(0.3), "complete(n)", size=11, color=TEAL)
    arrow(slide, I(10.8), I(2.9), I(11.4), I(2.9))
    arrow(slide, I(5.75), I(3.4), I(5.75), I(4.9), color=AMBER)
    text(slide, I(5.85), I(3.85), I(1.8), I(0.5), "실패-재시도(n)", size=11, color=INK)
    arrow(slide, I(4.55), I(5.35), I(2.2), I(3.4), color=AMBER)
    text(slide, I(3.75), I(4.95), I(1.5), I(0.3), "재개 시각", size=11, color=INK)
    arrow(slide, I(1.9), I(3.4), I(1.9), I(4.9), color=RED)
    text(slide, I(0.25), I(4.0), I(1.6), I(0.5), "시도 상한 초과", size=11, color=RED)
    for shape in (wait, claim, done, gone, delay, dead):
        shape.shadow.inherit = False
    box(slide, I(8.2), I(4.55), I(4.5), I(1.75), "", fill=MIST)
    text(slide, I(8.45), I(4.7), I(4.1), I(1.5), [("시도 횟수 = 서버가 승인한 착수 횟수", {"bold": True}), ("기한 지남·끊김도 이미 세어짐 → 무한 순환 없음", {"bullet": True}), ("상한: 최종 실패(소진) + dead letter 한 전이", {"bullet": True}), ("끊김을 빼고 세는 큐별 정책 (Q7)", {"bullet": True})], size=12)
    footer(slide, "제자리 변경: 단일 작성자가 소유하므로 불변일 필요가 없다")


def s09_expiry(prs, h):
    use(h)
    slide = slide_base(prs, "스케줄러 없는 회수: 등록이 기한을 확인한다", kicker="착수 기한")
    y = I(3.6)
    arrow(slide, I(0.8), y, I(12.6), y, color=LINE, width=2)
    text(slide, I(12.1), y + I(0.1), I(0.8), I(0.3), "시간", size=11, color=MUTED)
    marks = [
        (1.2, "A 착수", "번호 7 · 기한 10초", INK, PAPER, True),
        (3.9, "기한 도래", "아무것도 하지 않음", LINE, INK, False),
        (6.0, "등록·착수 요청 도착", "힙 맨 앞 확인 → 7 회수", AMBER, INK, True),
        (8.4, "B 착수", "번호 8", TEAL, PAPER, False),
        (10.6, "A의 늦은 완료(7)", "번호가 없다 → 무시", RED, PAPER, True),
    ]
    for x, title, sub, fill, color, up in marks:
        box(slide, I(x + 0.45), y - I(0.12), I(0.24), I(0.24), "", fill=fill, shape=MSO_SHAPE.OVAL)
        top = y - I(1.4) if up else y + I(0.4)
        box(slide, I(x), top, I(2.1), I(0.95), title, fill=fill if fill != LINE else MIST, color=color, size=13, bold=True, sub=sub)
    rows = [["규칙", "내용"],
            ["경계", "서버 시각 now ≥ deadline"],
            ["일괄 회수 (Q3)", "등록·착수 요청 배치에서 기한 힙의 맨 앞만. 지난 것이 없으면 비교 한 번"],
            ["대상 한 건 검사", "complete · extend · register_in은 그 레코드의 기한을 직접 확인"],
            ["오래된 힙 항목", "상태 · 번호 · 기한이 모두 같을 때만 적용 (연장된 착수 보호)"]]
    table(slide, MARGIN, I(5.2), I(12.1), rows, [1.6, 6.5], size=12, row_h=0.34)


def s10_fencing(prs, h):
    use(h)
    slide = slide_base(prs, "번호는 접속에 묶인다: 늦은 통지도, 낡은 자식도 통하지 않는다", kicker="펜싱")
    lanes = [("접속 c · 세대 1", 2.2, INK_2), ("이벤트 서버", 3.85, INK), ("접속 c · 세대 2 (재접속)", 5.5, TEAL)]
    for label, y, fill in lanes:
        box(slide, MARGIN, I(y), I(2.6), I(0.8), label, fill=fill, color=PAPER, size=13, bold=True)
        arrow(slide, I(3.2), I(y + 0.4), I(12.6), I(y + 0.4), color=LINE, width=1)
    arrow(slide, I(3.6), I(2.6), I(3.6), I(4.15))
    text(slide, I(3.7), I(3.1), I(1.8), I(0.5), "claim → 번호 7\n소유 (c, 1)", size=11)
    box(slide, I(5.2), I(2.35), I(1.6), I(0.5), "끊김", fill=RED_SOFT, color=RED, size=12, bold=True)
    arrow(slide, I(6.0), I(2.85), I(6.0), I(4.15), color=RED)
    text(slide, I(6.1), I(3.1), I(2.2), I(0.5), "(c,1)이 쥔 착수 즉시 대기로", size=11, color=RED)
    arrow(slide, I(8.0), I(5.9), I(8.0), I(4.65), color=TEAL)
    text(slide, I(8.1), I(4.95), I(2.0), I(0.5), "claim → 번호 9\n소유 (c, 2)", size=11, color=TEAL)
    arrow(slide, I(10.2), I(2.6), I(10.2), I(4.15), color=RED, dashed=True)
    text(slide, I(10.3), I(2.95), I(2.6), I(0.7), "세대 1의 늦은 종료 통지\n→ (c,2)의 착수는 그대로", size=11, color=RED)
    box(slide, MARGIN, I(6.45), I(12.1), I(0.5), "외부 효과(외부 API·과금)의 중복은 막지 않는다 → 재시도 사이에 안정된 작업 키로 수신 측이 막는다 (G3)", fill=AMBER_SOFT, size=12, bold=True)


def s10b_complete(prs, h):
    use(h)
    slide = slide_base(prs, "완료 보고의 거절은 '완료됐다'가 아니다", kicker="complete 판정")
    rows = [["판정", "뜻", "보고한 쪽이 할 일"],
            ["반영됨", "이번 요청으로 반영됐다", "끝"],
            ["이미 반영됨", "같은 완료 요청 id가 이미 반영됐다 (응답만 유실)", "끝"],
            ["효력 없음", "번호가 현재가 아니다 (만료·회수·다른 소유자). 완료됐다는 뜻이 아니다", "없음. 다른 착수자의 몫"],
            ["확인 불가", "완료 기록이 창 밖이라 이전 반영 여부를 알 수 없다", "작업 키로 외부 상태 확인"],
            ["자식 거절", "필수 자식이 충돌·권한·포화로 실패해 아무것도 바뀌지 않았다", "지연으로 반납, 허가 즉시 반환"]]
    table(slide, MARGIN, I(1.85), I(12.1), rows, [1.5, 5.6, 3.0], size=13, row_h=0.6)
    cases = [("A. 반영 후 ACK 유실", "같은 request_id로 재시도 → 이미 반영됨", TEAL_SOFT, TEAL), ("B. 반영 전 기한 지남", "재시도 → 효력 없음. 일은 다른 착수자가 다시 한다", RED_SOFT, RED)]
    width = (W - 2 * MARGIN - I(0.3)) / 2
    for index, (title, body, fill, color) in enumerate(cases):
        x = MARGIN + index * (width + I(0.3))
        box(slide, x, I(5.75), width, I(1.0), title, fill=fill, color=color, size=14, bold=True, sub=body)


def s11_dedup(prs, h):
    use(h)
    slide = slide_base(prs, "중복은 등록하는 순간 판정한다", kicker="등록 시점 판정")
    rows = [["같은 명령 키의 기존 상태", "판정", "등록자에게", "채널 효과"],
            ["없음", "신규", "신규", "레코드 생성 · 채널 대상은 로그 기록"],
            ["대기 · 착수 · 지연", "진행 중 중복", "진행 중 (원래 이벤트 id)", "없음. 결과는 원래 요청의 응답 채널로만"],
            ["완료 기록 (성공·실패·소진)", "완료 결과", "저장된 결과를 응답에", "재통지 id가 채널에 없을 때만 결과 하나 추가"],
            ["action·내용이 다름 (먼저 검사)", "충돌", "충돌", "등록자 응답 채널에 충돌 이벤트"]]
    table(slide, MARGIN, I(1.85), I(12.1), rows, [2.5, 1.3, 2.3, 3.6], size=13, row_h=0.62)
    notes = [("지금과 다른 점", "지금은 두 번째 요청의 begin 시점에 판정해 완료 전후 순서에 따라 응답이 달라진다. 제안은 등록 순간 확정된다."),
             ("가볍게", "내용 비교는 같은 키가 있을 때만. 완료 기록은 완료 때 한 번 계산한 식별값만 남기고 원본 payload는 버린다."),
             ("이름공간", "명령 키는 (등록 주체, 키). 결과·재통지 id에도 들어가 공유 채널에서 서로 가리지 않는다.")]
    width = (W - 2 * MARGIN - I(0.6)) / 3
    for index, (title, body) in enumerate(notes):
        x = MARGIN + index * (width + I(0.3))
        box(slide, x, I(5.2), width, I(1.6), "", fill=MIST)
        text(slide, x + I(0.2), I(5.3), width - I(0.4), I(1.45), [(title, {"bold": True, "size": 14}), body], size=12)


def s12_handoff(prs, h):
    use(h)
    slide = slide_base(prs, "긴 일은 작업 레코드로 넘기고, 워커는 즉시 돌아간다", kicker="작업 인계")
    lanes = [("이벤트 워커", 0.9, INK_2), ("이벤트 서버", 4.1, INK), ("작업 실행기 (별도 예산)", 7.3, TEAL), ("클라이언트 채널", 10.5, AMBER)]
    for label, x, fill in lanes:
        box(slide, I(x), I(1.75), I(2.1), I(0.6), label, fill=fill, color=PAPER if fill != AMBER else INK, size=12, bold=True)
        arrow(slide, I(x + 1.05), I(2.35), I(x + 1.05), I(5.5), color=LINE, width=1)
    steps = [
        (1.95, 5.15, 2.8, "① claim → 번호 n₁", INK_2),
        (1.95, 5.15, 3.4, "② complete(n₁, 인계, [작업 요청]) — 전부 또는 전무", INK_2),
        (8.35, 5.15, 4.0, "③ 작업 claim → 번호 n₂ (긴 lease)", TEAL),
        (8.35, 11.55, 4.6, "④ register_in(n₂, 진행)", TEAL),
        (8.35, 11.55, 5.2, "⑤ complete(n₂, 성공 또는 실패)", TEAL),
    ]
    for x1, x2, y, label, color in steps:
        arrow(slide, I(x1), I(y), I(x2), I(y), color=color)
        text(slide, I(min(x1, x2) + 0.1), I(y - 0.3), I(4.8), I(0.3), label, size=11, color=color, bold=True)
    box(slide, MARGIN, I(5.75), I(3.6), I(1.0), "자식 거절이면 아무것도 안 바뀜", fill=AMBER_SOFT, color=INK, size=13, bold=True, sub="충돌·권한·포화 → 부모를 지연으로 반납")
    box(slide, I(4.45), I(5.75), I(4.2), I(1.0), "실행기가 죽으면", fill=RED_SOFT, color=RED, size=14, bold=True, sub="끊김·기한으로 회수 → 다른 실행기가 이어받음")
    text(slide, I(8.95), I(5.8), I(3.8), I(1.0), ["실패도 이벤트로 등록한다", "침묵·panic만 감독자가 n₂로 실패 보고", "작업 큐에 수·바이트 상한, 넘으면 포화 거절"], size=11, color=MUTED, spacing=2)


def s13_fanout(prs, h):
    use(h)
    slide = slide_base(prs, "발송: 이벤트 서버의 비용은 프록시 수에만 비례한다", kicker="프록시와 팬아웃")
    box(slide, I(0.8), I(2.9), I(2.4), I(1.6), "이벤트 서버", fill=INK, color=PAPER, size=16, bold=True, sub="채널 → 관심 프록시")
    proxies = [2.0, 3.35, 4.7]
    for index, y in enumerate(proxies):
        box(slide, I(4.6), I(y), I(2.3), I(0.95), f"프록시 {index + 1}", fill=AMBER, size=13, bold=True, sub="관심 채널 집합")
        arrow(slide, I(3.2), I(3.7), I(4.6), I(y + 0.47))
        for row in range(3):
            box(slide, I(8.3 + row * 0.75), I(y + 0.22), I(0.5), I(0.5), "", fill=INK_2, shape=MSO_SHAPE.OVAL)
        arrow(slide, I(6.9), I(y + 0.47), I(8.3), I(y + 0.47), color=LINE)
    text(slide, I(3.3), I(2.35), I(1.3), I(0.5), "이벤트당\n한 번씩", size=11, color=MUTED)
    text(slide, I(8.1), I(5.75), I(2.6), I(0.3), "사용자별 발송은 프록시가", size=11, color=MUTED)
    items = [("관심 채널", "프록시는 채널별 사용자 수의 0→1, 1→0 전이만 위로 알린다"),
             ("잠금 밖 깨우기", "깨울 대상은 임계구역 안에서 확보, 통지는 잠금을 놓은 뒤"),
             ("느린 접속", "서버는 기다리지 않는다. 보존 한도를 넘으면 gap, 범위 읽기로 따라온다")]
    for index, (title, body) in enumerate(items):
        y = I(1.95 + index * 1.45)
        box(slide, I(10.9), y, I(2.0), I(1.25), "", fill=MIST)
        text(slide, I(11.0), y + I(0.1), I(1.85), I(1.1), [(title, {"bold": True, "size": 12}), body], size=10)


def s14_auth(prs, h):
    use(h)
    slide = slide_base(prs, "인증도 이벤트로: 프록시는 끝까지 로직 없는 중계기", kicker="인증·권한 (Q1 제안)")
    lanes = [("사용자", 1.0, INK_2), ("프록시", 4.0, AMBER), ("이벤트 서버", 7.0, INK), ("인증 서비스 (워커)", 10.0, TEAL)]
    for label, x, fill in lanes:
        box(slide, I(x), I(1.75), I(2.3), I(0.6), label, fill=fill, color=PAPER if fill != AMBER else INK, size=12, bold=True)
        arrow(slide, I(x + 1.15), I(2.35), I(x + 1.15), I(6.2), color=LINE, width=1)
    steps = [(2.15, 5.15, 2.8, "① 접속 + 토큰"), (5.15, 8.15, 3.35, "② 전용 큐에 인증 요청 (로그·팬아웃 없음)"), (11.15, 8.15, 3.9, "③ 인증 서비스만 착수 가능"),
             (11.15, 8.15, 4.45, "④ 결과: 요청·세션·세대·기한·범위"), (8.15, 5.15, 5.0, "⑤ 위임 신원과 범위 확정"), (5.15, 2.15, 5.55, "⑥ 이후 호출은 범위 안에서만")]
    for x1, x2, y, label in steps:
        arrow(slide, I(x1), I(y), I(x2), I(y))
        text(slide, I(min(x1, x2) + 0.1), I(y - 0.3), I(4.8), I(0.3), label, size=11, bold=True)
    box(slide, MARGIN, I(6.4), I(12.1), I(0.5), "재접속 뒤 도착한 옛 인증 결과는 세대가 달라 무효 · 권한이 다른 사용자는 사용자별 발송 단계에서 거른다", fill=MIST, size=12)


def s15_guarantees(prs, h):
    use(h)
    slide = slide_base(prs, "보장 계약: 설계보다 먼저 고정한다", kicker="G1 ~ G7")
    items = [("G1", "착수자는 최대 하나", "각 순간, 레코드마다 엔진이 인정하는 착수자"),
             ("G2", "낡은 번호는 효력 없음", "자식도 함께. 거절이 '완료됐다'는 뜻은 아니다"),
             ("G3", "외부 효과 중복 가능", "at-least-once. 작업 키로 수신 측이 막는다"),
             ("G4", "남은 일은 재시작을 넘는다", "Scheduled는 마지막 쓰기 이후 손실 가능, Strict·flush는 응답 전 기록"),
             ("G5", "인계한 일은 남는다", "넘긴 워커가 죽어도 레코드로 책임 유지 (서버까지 죽으면 G4)"),
             ("G6", "소켓을 기다리지 않는다", "뒤처진 접속은 이벤트가 없어도 gap을 받는다"),
             ("G7", "등록마다 판정은 하나", "신규 · 진행 중 중복 · 완료 결과 · 충돌")]
    width = (W - 2 * MARGIN - I(0.9)) / 4
    for index, (tag, title, body) in enumerate(items):
        col, row = index % 4, index // 4
        x = MARGIN + col * (width + I(0.3))
        y = I(1.85) + row * I(2.5)
        box(slide, x, y, width, I(2.2), "", fill=TEAL_SOFT if tag != "G3" else AMBER_SOFT)
        text(slide, x + I(0.2), y + I(0.15), I(1.2), I(0.6), tag, size=28, bold=True, color=TEAL if tag != "G3" else AMBER)
        text(slide, x + I(0.2), y + I(0.8), width - I(0.4), I(0.6), title, size=14, bold=True)
        text(slide, x + I(0.2), y + I(1.45), width - I(0.4), I(0.7), body, size=11, color=MUTED)
    x = MARGIN + 3 * (width + I(0.3))
    box(slide, x, I(4.35), width, I(2.2), "각 단계의 인수 테스트가\n이 계약을 판정한다", fill=INK, color=PAPER, size=14, bold=True)


def s16_journal(prs, h):
    use(h)
    slide = slide_base(prs, "저널 v2: 재생 뒤 무엇이 같아야 하는가부터", kicker="복구 동치")
    rows = [["복구 대상", "재생 후"],
            ["대기 · 착수 레코드", "모두 대기. 소유자와 번호는 무효"],
            ["지연 레코드", "지연 유지. 재개 시각을 기록해 백오프 보존"],
            ["시도 횟수", "유지 (재시작은 더하지 않음)"],
            ["자식(리액션)", "원인과 같은 스텝에 기록된 그대로. 다시 생성하지 않음"],
            ["리액션 변경 · dead letter", "유지"],
            ["완료 기록 · 이벤트", "정책에 따라"]]
    table(slide, MARGIN, I(1.85), I(6.6), rows, [2.4, 4.2], size=13, row_h=0.55)
    sources = [("순변화 재생", 2.0), ("압축본 재생", 3.35), ("실제 재시작", 4.7)]
    for label, y in sources:
        box(slide, I(7.8), I(y), I(2.3), I(0.9), label, fill=MIST, line=LINE, size=14, bold=True)
        arrow(slide, I(10.1), I(y + 0.45), I(10.9), I(3.8))
    box(slide, I(10.9), I(3.2), I(1.9), I(1.2), "같은 상태", fill=TEAL, color=PAPER, size=16, bold=True)
    text(slide, I(7.8), I(5.85), I(5.0), I(1.0), ["같은 질의: claim 결과 · 같은 키 재등록 판정 · 같은 커서의 read와 gap", "허용 차이: 보존선 이하의 옛 이벤트는 다시 보이거나 gap"], size=12, color=MUTED)
    text(slide, MARGIN, I(5.95), I(6.6), I(0.9), ["순변화 · 쓰기 비중첩 · 사슬 · 압축 규칙은 유지", "형식 버전 2. v1 파일은 거부하고 바이트를 보존"], size=12, color=MUTED)


def s17_hotpath(prs, h):
    use(h)
    slide = slide_base(prs, "핫패스: 측정한 뒤 바꾼다", kicker="이벤트당 비용")
    rows = [["#", "지금", "바꿀 것", "분류"],
            ["1", "단순 성공 명령이 로그에 명령·결과 두 행", "명령 행을 남기지 않음", "Q4 결정"],
            ["2·3·10", "메시지·실행 표 이중, begin 시점 판정, dequeue·begin 왕복", "레코드 하나 · 등록 시점 판정 · claim", "API 계약상 필요"],
            ["4", "begin마다 action·payload 해시", "같은 키가 있을 때만", "6.5에서 선택"],
            ["5·6·7", "배치마다 모든 큐·대기자 순회", "바뀐 큐만 · 기한 힙", "P0 측정 → P1"],
            ["8", "잠금 중 이벤트마다 깨우기", "채널당 배치당 한 번, 잠금 밖", "P0 측정 → P1"],
            ["9", "실행당 태스크 둘", "future 단위 panic 포착", "P0 측정 → P1"],
            ["11", "응답마다 클로저 박싱", "응답 목록을 값으로 순회", "P0 측정 → P1"]]
    table(slide, MARGIN, I(1.85), I(12.1), rows, [0.8, 4.6, 3.6, 1.6], size=12, row_h=0.52)
    text(slide, MARGIN, I(6.2), I(12), I(0.6), "이벤트 한 건의 비용이 서버 전체의 비용이다. 현행 항목은 P0에서 재고 정당화된 것만 P1에서 바꾼다. 착수 기한 힙은 현행에 없으므로 P2 후보 구현에서 잰다.", size=12, color=MUTED)


def s18_plan(prs, h):
    use(h)
    slide = slide_base(prs, "단계 계획: 측정부터, 단계마다 증거로 닫는다", kicker="P0 ~ P6")
    phases = [("P0", "현행 기준선", "Scheduled·Strict·휘발. 구독자·대기자·배치별 CPU·할당·p99"),
              ("P1", "깨지지 않는 개선", "저널 실패로 run이 실패 종료. 깨우기 누락 결정적 시험"),
              ("P2", "레코드·API·저널 v2", "선행: Q1·Q3~Q8. 참조 모델 대조, complete 판정, 인계 원자성, 복구 동치"),
              ("P3", "워커를 API 위로", "선행: Q2. 과부하 중 큐 상한·허가 반환·타 서비스 처리량"),
              ("P4", "원격 프로토콜", "로컬=원격, 인계 직후 워커만 종료, ACK 유실 양방향"),
              ("P5", "프록시 바이너리", "사용자↑·프록시 고정과 반대, 단독 gap, 인증 역순"),
              ("P6", "apibondrust", "외부 API 실패가 최종 실패 이벤트 하나로")]
    width = (W - 2 * MARGIN - I(0.6)) / 7
    for index, (tag, title, proof) in enumerate(phases):
        x = MARGIN + index * (width + I(0.1))
        box(slide, x, I(1.9), width, I(0.9), tag, fill=INK if index else AMBER, color=PAPER if index else INK, size=20, bold=True, shape=MSO_SHAPE.CHEVRON if index else MSO_SHAPE.PENTAGON)
        text(slide, x + I(0.05), I(3.0), width - I(0.1), I(0.7), title, size=14, bold=True)
        text(slide, x + I(0.05), I(3.75), width - I(0.1), I(2.3), proof, size=11, color=MUTED)
    box(slide, MARGIN, I(6.15), I(12.1), I(0.6), "각 단계의 끝: cargo xtask verify(전 크레이트) · 원문 인용 인수 테스트 · G1~G7 판정 · 문서 · 커밋", fill=TEAL_SOFT, color=TEAL, size=13, bold=True)


def s19_risks(prs, h):
    use(h)
    slide = slide_base(prs, "위험과 대가", kicker="알고 승인할 것")
    rows = [["위험", "내용", "대응"],
            ["API 파괴", "Begin, dequeue/begin, 게이트웨이 훅이 바뀐다", "0.x. API 고정본 차이를 보고"],
            ["응답 계약 변경", "등록 시점 판정으로 중복 요청자의 관측이 바뀐다", "의도된 변경. 인수 테스트로 고정"],
            ["저널 형식 변경", "v1을 읽지 못한다", "운영 데이터 없는 단계. v1 고정본 보관"],
            ["외부 효과 중복", "회수 뒤 재실행으로 외부 효과가 두 번", "G3 계약. 작업 키 노출"],
            ["끊김 소진", "끊김이 잦은 환경에서 정상 일이 소진 실패", "Q7 큐별 정책"],
            ["단일 엔진 천장", "큐 연산은 한 코어", "이벤트당 비용 절감 우선. 키 단위 분할은 계획 밖"],
            ["회수 몰림", "한산한 뒤 첫 배치가 만료를 몰아 처리", "P2 후보 측정. 필요하면 배치당 상한"],
            ["원격화 왕복", "원격 워커는 왕복이 생긴다", "claim 여유분 밀어 주기와 배치"]]
    table(slide, MARGIN, I(1.85), I(12.1), rows, [1.8, 4.4, 4.0], size=12, row_h=0.52)


def s20_decisions(prs, h):
    use(h)
    slide = slide_base(prs, "승인 때 정할 것: 미결정 여덟 가지", kicker="Q1 ~ Q8")
    items = [("Q1", "최종 사용자 인증", "전용 큐의 인증 서비스", "선언적 규칙 · 정적 토큰"),
             ("Q2", "이벤트 워커 스텝 기한", "30초, 서비스별 변경", "5초 · 필수 지정"),
             ("Q3", "일괄 회수 시점", "등록 + 착수 요청 배치", "등록만 · 유휴 시 한 번 깨어남"),
             ("Q4", "명령 이벤트 로그 기록", "남기지 않음", "채널 규칙으로 선택"),
             ("Q5", "v1 저널 이전", "하지 않음", "변환 도구"),
             ("Q6", "휘발 작업 허용", "선언 시 허용", "모두 복구 대상"),
             ("Q7", "끊김을 시도로 세기", "모두 셈 + 큐별 예외", "시작 확인 왕복 추가"),
             ("Q8", "리액션 갱신 경합", "소유 서비스별 세대 비교", "갱신 주체 하나")]
    width = (W - 2 * MARGIN - I(0.9)) / 4
    for index, (tag, title, pick, other) in enumerate(items):
        col, row = index % 4, index // 4
        x = MARGIN + col * (width + I(0.3))
        y = I(1.85) + row * I(2.4)
        box(slide, x, y, width, I(2.2), "", fill=MIST)
        chip(slide, x + I(0.2), y + I(0.2), tag, fill=INK, color=PAPER)
        text(slide, x + I(0.2), y + I(0.65), width - I(0.4), I(0.45), title, size=14, bold=True)
        text(slide, x + I(0.2), y + I(1.1), width - I(0.4), I(1.05), [("제안: " + pick, {"bold": True, "color": TEAL}), ("대안: " + other, {"color": MUTED})], size=11)
    footer(slide, "P2 착수 전에 Q1 · Q3~Q8, P3 전에 Q2가 정해져야 한다")


def s21_review(prs, h):
    use(h)
    slide = slide_base(prs, "외부 리뷰 두 번을 거쳤다", kicker="Codex gpt-6.2-astra · high")
    from review_numbers import ROUNDS  # filled from the review reports

    width = (W - 2 * MARGIN - I(0.3)) / 2
    for index, round_ in enumerate(ROUNDS):
        x = MARGIN + index * (width + I(0.3))
        box(slide, x, I(1.85), width, I(4.7), "", fill=MIST)
        text(slide, x + I(0.3), I(2.0), width - I(0.6), I(0.45), round_["title"], size=18, bold=True)
        stats = round_["stats"]
        stat_w = (width - I(0.6)) / len(stats)
        for s, (label, value, color) in enumerate(stats):
            sx = x + I(0.3) + s * stat_w
            text(slide, sx, I(2.6), stat_w, I(0.8), str(value), size=40, bold=True, color=color)
            text(slide, sx, I(3.45), stat_w, I(0.3), label, size=11, color=MUTED)
        text(slide, x + I(0.3), I(4.0), width - I(0.6), I(2.4), [(line, {"bullet": True, "after": 6}) for line in round_["lines"]], size=12)
    footer(slide, "전문: target/codex-review/r1, r2 의 report.json · 반영 기록: 제안서 12장")


def s22_ask(prs, h):
    use(h)
    slide = slide_base(prs, "승인을 요청합니다", kicker="다음 단계", dark=True)
    text(slide, MARGIN, I(1.9), I(7.4), I(3.2), [("승인하면", {"bold": True, "color": AMBER, "size": 18}),
                                                 ("P0 측정부터 순서대로 진행", {"bullet": True}),
                                                 ("단계마다 측정 · 테스트 · 차이를 보고", {"bullet": True}),
                                                 ("P2 전에 Q1 · Q3~Q8, P3 전에 Q2 확정", {"bullet": True}),
                                                 ("원문과 충돌하는 판단이 생기면 멈추고 묻는다", {"bullet": True})], size=18, color=PAPER, spacing=10)
    box(slide, I(8.6), I(1.9), I(4.1), I(3.6), "", fill=INK_2)
    text(slide, I(8.85), I(2.1), I(3.6), I(3.3), [("함께 보는 문서", {"bold": True, "color": AMBER}), "docs/proposals/", "event-server-redesign.md", "", ("이 덱", {"bold": True, "color": AMBER}), "event-server-redesign.pptx", "(생성기: deck/build.py)"], size=14, color=PAPER)
    footer(slide, "승인 전에는 코드를 바꾸지 않는다", dark=True)


SLIDES = [s01_title, s02_summary, s03_now, s04_diagnosis, s05_principles, s06_target, s07_api, s08_states, s09_expiry, s10_fencing, s10b_complete, s11_dedup, s12_handoff, s13_fanout, s14_auth, s15_guarantees, s16_journal, s17_hotpath, s18_plan, s19_risks, s20_decisions, s21_review, s22_ask]
