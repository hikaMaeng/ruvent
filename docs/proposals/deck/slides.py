"""Slide content for the redesign deck (3rd revision). Each function draws one slide."""

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
    text(slide, MARGIN, I(4.05), I(7.6), I(1.2), ["메서드 일곱 개 · 착수용 큐와 발송용 큐", "정책은 워커가 · 스케줄러 없는 착수 회수"], size=18, color=LINE)
    text(slide, MARGIN, I(6.55), I(9), I(0.3), "기준 커밋 f5a1439 · 3차 개정 · 외부 리뷰(Codex) 3회 · 2026-11-22", size=12, color=LINE)
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
        ("지금의 한계", RED_SOFT, RED, ["워커가 긴 일을 붙잡아 풀이 멈출 수 있다", "착수에 기한이 없어 죽은 워커의 일이 묶인다", "서버가 정책(재시도·dead letter·보존 로그)을 갖는다", "모든 접속이 한 프로세스에 붙는다"]),
        ("바꿀 구조", TEAL_SOFT, TEAL, ["이벤트 서버는 메서드 일곱 개뿐", "착수용 큐는 완료로, 발송용 큐는 발송으로 비운다", "엔벨롭 하나에 id·채널·재시도·착수 정보", "재시도·실패·후속은 워커 로직", "프로토콜별 프록시 바이너리가 클라이언트를 받는다"]),
        ("승인할 것", AMBER_SOFT, AMBER, ["보장 계약 G1~G8", "단계 계획 P0~P6 (측정부터)", "미결정 Q1~Q11의 방향"]),
    ]
    width = (W - 2 * MARGIN - I(0.6)) / 3
    for index, (title, fill, accent, lines) in enumerate(cards):
        x = MARGIN + index * (width + I(0.3))
        box(slide, x, I(1.85), width, I(4.6), "", fill=fill)
        text(slide, x + I(0.3), I(2.1), width - I(0.6), I(0.5), title, size=22, bold=True, color=accent)
        text(slide, x + I(0.3), I(2.85), width - I(0.6), I(3.5), [(line, {"bullet": True, "after": 10}) for line in lines], size=15)
    footer(slide, FOOT)


def s03_now(prs, h):
    use(h)
    slide = slide_base(prs, "지금 구조: 모든 것이 한 프로세스 안에", kicker="현재 (f5a1439)")
    box(slide, I(0.6), I(1.75), I(8.6), I(4.95), "", fill=MIST, line=LINE)
    text(slide, I(0.85), I(1.9), I(4), I(0.3), "한 프로세스", size=13, bold=True, color=MUTED)
    box(slide, I(3.6), I(2.55), I(2.6), I(1.5), "엔진", fill=INK, color=PAPER, size=18, bold=True, sub=["단일 작성자", "큐 · 실행 · 보존 로그"])
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
    marks = [(0.9, 4.05, "D1 허가를 끝까지 쥠 · 기한 600초"), (0.9, 4.55, "D3 착수에 기한 없음"), (0.9, 5.05, "D11 서버가 재시도·dead letter·리액션"), (0.9, 5.55, "D12 클라이언트 이벤트를 보존 한도로 지움"),
             (6.5, 4.2, "D7 잠금 중 구독자 깨우기"), (6.5, 4.7, "D8 엔진이 멈춰도 프로세스는 산다"), (6.5, 5.2, "D13 저널 flush·compact가 메서드")]
    for x, y, label in marks:
        box(slide, I(x), I(y), I(2.6), I(0.4), label, fill=RED_SOFT, color=RED, size=10, bold=True, align=PP_ALIGN.LEFT)
    text(slide, I(9.6), I(5.4), I(3.2), I(1.2), ["측정(91042f3): 왕복 15만/초", "3단 연쇄 3만/초 · Strict 왕복 4만/초"], size=12, color=MUTED)
    footer(slide, FOOT)


def s04_diagnosis(prs, h):
    use(h)
    slide = slide_base(prs, "진단: 목표와 어긋나는 여섯 지점", kicker="왜 바꾸나")
    cells = [
        ("D1", "워커가 긴 일을 붙잡는다", "허가를 완료 응답까지 보유, 기한 600초. 그 서비스 풀이 멈춘다"),
        ("D2·D3", "넘길 길도, 회수할 길도 없다", "넘긴 Context의 진행은 소리 없이 버려지고, 착수는 기한 없이 묶인다"),
        ("D4·D5", "내장 워커만 다른 길을 쓴다", "엔진 전용 메서드라 외부 워커를 같은 코드로 못 만든다"),
        ("D7", "발송 비용이 엔진 경로에", "게시 중 잠금을 쥐고 채널 구독자를 이벤트마다 깨운다"),
        ("D11", "서버가 정책을 갖는다", "시도 상한·dead letter·리액션 표·중복 판정. 원문은 모두 워커 로직"),
        ("D12", "언제 지울지 모른다", "클라이언트용 이벤트를 개수·바이트·시간 한도로 지운다"),
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
    text(slide, MARGIN, I(6.75), I(12), I(0.3), "그 밖: D6 게이트웨이 사용자 훅 · D8 장애가 드러나지 않음 · D9 핫패스 · D10 Events::Custom · D13 저널이 메서드", size=12, color=MUTED)


def s05_principles(prs, h):
    use(h)
    slide = slide_base(prs, "판정 기준은 사용자 원문이다", kicker="원칙")
    quotes = [
        "모든 워커는 재빨리 워커풀로 돌아와야해. 따라서 장기실행을 이벤트용 워커가 들고 있을 수는 없어.",
        "착수이벤트의 타임아웃은 … 스케줄러가 필요없어. … 이벤트 등록시점에 … 일괄 미착수 상태로 돌리기만 하면 되기 때문이야.",
        "발송전용 이벤트는 발송을 통해 큐에서 제거되어야하기 때문이다. 아니면 언제 큐에서 제거할지 판단하기 어렵다.",
        "일정 재시도 횟수가 넘어가면 … 전부 해당 이벤트를 처리할 워커 내부로직으로 해결",
        "저널링은 메소드가 아니다. … 미처리 이벤트를 메모리에 로드하기 위한 목적을 최우선으로",
        "ID가 같은 이벤트는 등록될 수 없고 ID는 영원히 고유하지 않아도 충분하지만 비용이 저렴한 UUID7 등을 …",
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
    box(slide, I(5.6), I(2.15), I(3.0), I(3.6), "", fill=INK)
    text(slide, I(5.85), I(2.3), I(2.6), I(0.4), "이벤트 서버", size=18, bold=True, color=PAPER)
    text(slide, I(5.85), I(2.8), I(2.6), I(0.6), ["메서드 일곱 개 · 단일 작성자", "정책·사용자 코드 없음"], size=11, color=LINE, spacing=2)
    box(slide, I(5.85), I(3.5), I(2.5), I(0.55), "착수용 큐 (완료로 제거)", fill=TEAL, color=PAPER, size=11, bold=True)
    box(slide, I(5.85), I(4.15), I(2.5), I(0.55), "발송용 큐 (발송으로 제거)", fill=AMBER, color=INK, size=11, bold=True)
    box(slide, I(5.85), I(4.85), I(2.5), I(0.7), "내장 워커", fill=INK_2, color=PAPER, size=12, line=AMBER, sub="같은 메서드를 함수로")
    servers = [("워커 서버 A", 2.3), ("워커 서버 B", 4.1)]
    for label, y in servers:
        box(slide, I(10.0), I(y), I(2.6), I(1.2), label, fill=TEAL, color=PAPER, size=14, bold=True, sub=["워커풀 여러 개", "재시도·실패 정책"])
        arrow(slide, I(10.0), I(y + 0.6), I(8.6), I(3.95), color=INK_2, both=True)
    box(slide, I(5.4), I(6.0), I(7.2), I(0.6), "이벤트 서버의 연결 수 = 프록시 수 + 워커 서버 수 (사용자 수와 무관)", fill=TEAL_SOFT, color=TEAL, size=13, bold=True)
    footer(slide, FOOT)


def s07_methods(prs, h):
    use(h)
    slide = slide_base(prs, "이벤트 서버의 메서드는 일곱 개뿐이다", kicker="메서드")
    rows = [["#", "메서드", "하는 일", "응답"],
            ["1", "착수용 이벤트 등록", "대기로 넣고 작업 키 부여. 같은 id가 큐에 있으면 거절. 직전에 기한 회수. 참조 알림", "id·키 / 거절"],
            ["2", "발송용 이벤트 등록", "직전에 기한 회수. 큐의 id와 같으면 거절. 이 순간의 구독 접속 버퍼에 넣고 끝", "접수 / 거절"],
            ["3", "착수 마킹 (id, 워커, 기간)", "대기면 엔벨롭의 착수 블록을 제자리에서 채우고 착수 표지 발급", "엔벨롭 / 실패"],
            ["4", "완료 보고 (id, 표지, 후속)", "id·표지·서비스 자격이 맞고 후속이 모두 유효하면 지우고 후속 등록", "완료 / 무효"],
            ["5", "삭제 (id, 결과 채널[, 키])", "대기면 지운다(키를 주면 그 등록 건만). 결과 채널에 통지 한 건", "성공 / 실패"],
            ["6", "채널 구독 ([이어받을 키])", "구독자로 등록. 착수용 채널이면 대기분을 상한까지, 넘치면 이어받기", "접수 (대기분)"],
            ["7", "채널 구독 취소", "구독자에서 뺀다. 이 처리 이후의 발송은 받지 않음", "접수"]]
    table(slide, MARGIN, I(1.8), I(12.1), rows, [0.4, 2.5, 7.0, 1.6], size=12, row_h=0.52)
    box(slide, MARGIN, I(6.15), I(12.1), I(0.6), "저널링 · 스냅샷 · 압축은 메서드가 아니다 — 서버가 설정대로 스스로 한다", fill=AMBER_SOFT, size=14, bold=True)


def s08_lifecycle(prs, h):
    use(h)
    slide = slide_base(prs, "착수용 이벤트: 등록 → 착수 마킹 → 완료 보고", kicker="라이프사이클")
    box(slide, I(0.6), I(2.5), I(1.6), I(0.9), "등록", fill=PAPER, line=LINE, size=16, bold=True)
    wait = box(slide, I(2.9), I(2.4), I(2.2), I(1.1), "대기", fill=MIST, line=LINE, size=18, bold=True, sub="재시도 카운트 n")
    box(slide, I(6.0), I(2.4), I(3.0), I(1.1), "착수", fill=INK, color=PAPER, size=18, bold=True, sub="엔벨롭 안 착수 블록 기록")
    box(slide, I(10.0), I(2.5), I(2.7), I(0.9), "완료 보고 → 제거", fill=TEAL_SOFT, color=TEAL, size=15, bold=True)
    arrow(slide, I(2.2), I(2.95), I(2.9), I(2.95))
    arrow(slide, I(5.1), I(2.8), I(6.0), I(2.8))
    text(slide, I(5.15), I(2.45), I(0.85), I(0.3), "착수 마킹", size=10, color=MUTED)
    arrow(slide, I(9.0), I(2.95), I(10.0), I(2.95), color=TEAL)
    arrow(slide, I(7.5), I(3.5), I(7.5), I(4.4), color=RED)
    arrow(slide, I(7.5), I(4.4), I(4.0), I(4.4), color=RED)
    arrow(slide, I(4.0), I(4.4), I(4.0), I(3.5), color=RED)
    text(slide, I(4.2), I(4.5), I(3.6), I(0.6), ["기한 지남 (등록 처리 직전에 확인)", "재시도 +1 · id 재발급 · 표지 무효"], size=12, color=RED, bold=True)
    box(slide, I(0.6), I(4.3), I(2.6), I(0.75), "삭제 (대기 중일 때만)", fill=RED_SOFT, color=RED, size=12, bold=True)
    arrow(slide, I(2.9), I(3.3), I(2.2), I(4.3), color=RED)
    items = [("완료는 id + 착수 표지", "경합 패자 · 회수된 옛 착수 · 재기동 전 워커의 보고는 무효. 실패도 완료로 끝내고 이후는 새 이벤트"),
             ("상한은 워커가", "재시도 카운트를 보고 계속할지, 새 이벤트를 낼지, 실패 보고를 할지 워커 로직이 정한다"),
             ("서버에서 빠지는 것", "시도 상한 · dead letter · 백오프 상태 · 실패 결과 자동 생성 · 리액션 표")]
    width = (W - 2 * MARGIN - I(0.6)) / 3
    for index, (title, body) in enumerate(items):
        x = MARGIN + index * (width + I(0.3))
        box(slide, x, I(5.45), width, I(1.35), "", fill=MIST)
        text(slide, x + I(0.2), I(5.55), width - I(0.4), I(1.2), [(title, {"bold": True, "size": 14}), body], size=12)


def s09_expiry(prs, h):
    use(h)
    slide = slide_base(prs, "스케줄러 없는 회수: 등록이 기한을 확인한다", kicker="착수 기한")
    y = I(3.6)
    arrow(slide, I(0.8), y, I(12.6), y, color=LINE, width=2)
    text(slide, I(12.1), y + I(0.1), I(0.8), I(0.3), "시간", size=11, color=MUTED)
    marks = [
        (1.2, "A 착수 마킹", "id X · 기한 10초", INK, PAPER, True),
        (3.9, "기한 도래", "아무것도 하지 않음", LINE, INK, False),
        (6.0, "등록 도착", "힙 맨 앞 확인 → X 회수", AMBER, INK, True),
        (8.4, "B 착수 마킹", "새 id Y · 재시도 1", TEAL, PAPER, False),
        (10.6, "A의 늦은 완료(X)", "X도 표지도 없다 → 무시", RED, PAPER, True),
    ]
    for x, title, sub, fill, color, up in marks:
        box(slide, I(x + 0.45), y - I(0.12), I(0.24), I(0.24), "", fill=fill, shape=MSO_SHAPE.OVAL)
        top = y - I(1.4) if up else y + I(0.4)
        box(slide, I(x), top, I(2.1), I(0.95), title, fill=fill if fill != LINE else MIST, color=color, size=13, bold=True, sub=sub)
    rows = [["규칙", "내용"],
            ["회수 시점", "등록(착수용·발송용)을 처리하기 직전, 배치당 한 번. 힙의 맨 앞만 본다"],
            ["회수하면", "재시도 +1, id 재발급(활성 id와 대조), 표지 무효, 대기로. 새 id로 다시 알림"],
            ["회수 전 늦은 완료 (Q10)", "표지가 맞으면 받는다. 회수 전이면 다른 워커가 가져갈 수 없었다"],
            ["오래된 힙 항목", "그 이벤트가 아직 같은 표지로 착수 중일 때만 적용. 작업 키·대기 순서는 유지"]]
    table(slide, MARGIN, I(5.2), I(12.1), rows, [1.7, 8.0], size=12, row_h=0.34)


def s10_two_queues(prs, h):
    use(h)
    slide = slide_base(prs, "두 종류의 이벤트, 두 개의 큐", kicker="착수용 · 발송용")
    width = (W - 2 * MARGIN - I(0.4)) / 2
    cols = [("착수용 큐", TEAL, TEAL_SOFT, ["받는 쪽: 착수 마킹에 성공한 워커 하나", "제거: 완료 보고 · 삭제", "기한: 착수 때 워커가 정함", "저널: 미처리분 기록, 재기동 때 다시 올림", "같은 id는 큐에 있는 동안 등록 불가"]),
            ("발송용 큐", AMBER, AMBER_SOFT, ["받는 쪽: 발송 시점의 채널 구독자 전부", "제거: 발송하는 그 순간", "구독자가 없으면 그냥 사라짐", "저널: 기록 안 함 (설정한 채널만 스냅샷)", "엔벨롭은 착수용과 같은 형식, 고정 머리만"])]
    for index, (title, accent, fill, lines) in enumerate(cols):
        x = MARGIN + index * (width + I(0.4))
        box(slide, x, I(1.85), width, I(0.75), title, fill=accent, color=PAPER if accent == TEAL else INK, size=20, bold=True)
        box(slide, x, I(2.7), width, I(3.0), "", fill=fill)
        text(slide, x + I(0.35), I(2.95), width - I(0.7), I(2.7), [(line, {"bullet": True, "after": 10}) for line in lines], size=15)
    box(slide, MARGIN, I(5.95), I(12.1), I(0.75), "클라이언트도 보고 워커도 처리해야 하는 사실이면 이벤트를 두 개 등록한다 (이벤트 하나 = 워커 하나 또는 발송)", fill=MIST, size=14, bold=True)


def s10c_envelope(prs, h):
    use(h)
    slide = slide_base(prs, "엔벨롭은 하나: 착수 정보까지 이벤트 안에", kicker="이벤트 형식")
    scale = 0.2  # inches per byte; narrow fields get a minimum width so their names fit
    rows = [("고정 머리 27", "모든 이벤트", [("id", 16, INK, "UUIDv7"), ("channel", 4, TEAL, "채널명"), ("type", 4, TEAL, ""), ("f", 1, MUTED, ""), ("retry", 2, AMBER, "")]),
            ("작업 키 8", "착수용 · 서버가 부여", [("세대 · 순번", 8, INK_2, "등록 건마다 · FIFO")]),
            ("착수 블록 24", "착수 중 · 제자리 기록", [("착수 표지", 8, RED, "세대 · 순번"), ("착수자", 4, RED, "귀속 번호"), ("워커", 4, RED, ""), ("기한", 8, RED, "u64 밀리초")])]
    for row, (label, sub, fields) in enumerate(rows):
        y = I(1.85 + row * 1.05)
        text(slide, MARGIN, y + I(0.02), I(2.4), I(0.6), [(label, {"bold": True, "size": 14}), (sub, {"size": 10, "color": MUTED})], size=14)
        x = I(3.0)
        for name, size, fill, note in fields:
            width = I(0.25) if size == 1 else I(max(size * scale, 0.7))
            box(slide, x, y, width, I(0.6), name, fill=fill, color=PAPER if fill in (INK, INK_2, TEAL, MUTED, RED) else INK, size=11, bold=True)
            if note:
                text(slide, x, y + I(0.63), width, I(0.3), note, size=9, color=MUTED, align=PP_ALIGN.CENTER)
            x += width
    y = I(1.85)
    box(slide, I(9.1), y, I(1.6), I(0.6), "선택 필드", fill=PAPER, line=LINE, size=11)
    box(slide, I(10.9), y, I(1.8), I(0.6), "payload", fill=MIST, line=LINE, size=11, bold=True)
    text(slide, I(9.1), y + I(0.63), I(3.6), I(0.3), "reply · correlation · principal (쓸 때만)", size=9, color=MUTED, align=PP_ALIGN.CENTER)
    text(slide, I(3.0), I(4.85), I(9.7), I(0.3), "f = flags: 종류(착수용·발송용) · 착수 여부 · 선택 필드 비트 · 예약(켜지면 거절)    retry는 65,535에서 멈춤", size=11, color=MUTED)
    stats = [("31", "발송용"), ("39", "착수용 대기"), ("63", "착수용 착수 중")]
    for index, (value, label) in enumerate(stats):
        x = MARGIN + index * I(1.75)
        text(slide, x, I(5.3), I(1.7), I(0.7), value, size=34, bold=True, color=AMBER)
        text(slide, x, I(6.0), I(1.7), I(0.5), [label, "+ payload"], size=10, color=MUTED)
    box(slide, I(6.0), I(5.3), I(6.7), I(1.5), "", fill=MIST)
    text(slide, I(6.2), I(5.38), I(6.4), I(1.4), [("착수 정보는 작은 숫자뿐: 서버는 상대가 워커인지 프록시인지 묻지 않는다", {"bullet": True}),
                                                  ("제자리 변경은 서버 사본만 · 인계할 때 머리 값만 복사, payload는 공유", {"bullet": True}),
                                                  ("기한은 기간으로 받아 서버 시계로 · 착수 블록은 저널에 쓰지 않음", {"bullet": True}),
                                                  ("세대는 단조 증가 · 순번 소진 때 세대를 올린다 · seq 없음", {"bullet": True})], size=11, spacing=3)
    footer(slide, "인코딩 바이트이며 상주 메모리 절감량이 아니다 · 상주·보존·저널 바이트는 P0·P2에서 측정")


def s11_ids(prs, h):
    use(h)
    slide = slide_base(prs, "id 규칙과 삭제", kicker="UUIDv7 · 중복 거절 · 삭제 결과")
    box(slide, MARGIN, I(1.85), I(5.9), I(4.85), "", fill=MIST)
    text(slide, MARGIN + I(0.3), I(2.0), I(5.3), I(0.5), "id", size=20, bold=True, color=TEAL)
    text(slide, MARGIN + I(0.3), I(2.6), I(5.3), I(4.0), [("형식: UUIDv7 — 앞 48비트 밀리초 시각 + 난수", {"bullet": True}),
                                                         ("큐에 있는 id와 같은 id는 착수용·발송용·후속 모두 거절", {"bullet": True}),
                                                         ("영원히 고유할 필요 없음: 큐를 떠난 id는 다시 쓸 수 있고, 서버는 기억하지 않음", {"bullet": True}),
                                                         ("등록자가 줄 수 있고, 없으면 서버가 발급 (Q5)", {"bullet": True}),
                                                         ("회수 때 재발급하는 id는 활성 id와 대조해 겹치면 다시 뽑는다", {"bullet": True}),
                                                         ("작업 키는 id와 별개로 등록 건마다: 같은 id를 다시 써도 키가 다르다", {"bullet": True})], size=14, spacing=10)
    x = MARGIN + I(6.2)
    text(slide, x, I(1.95), I(5.9), I(0.5), "삭제", size=20, bold=True, color=RED)
    cases = [("대기 중", "지운다 → 발송용 성공 이벤트", TEAL_SOFT, TEAL), ("이미 착수됨", "지우지 않는다 → 발송용 실패 이벤트", RED_SOFT, RED), ("없음 (완료·회수)", "→ 발송용 실패 이벤트", RED_SOFT, RED)]
    for index, (state, result, fill, color) in enumerate(cases):
        y = I(2.6 + index * 1.15)
        box(slide, x, y, I(2.2), I(0.95), state, fill=fill, color=color, size=14, bold=True)
        arrow(slide, x + I(2.2), y + I(0.47), x + I(2.7), y + I(0.47))
        box(slide, x + I(2.7), y, I(3.2), I(0.95), result, fill=PAPER, line=LINE, size=12)
    text(slide, x, I(6.1), I(5.9), I(0.6), "결과 이벤트는 삭제를 요청한 쪽이 받는 채널로 간다", size=12, color=MUTED)


def s12_handoff(prs, h):
    use(h)
    slide = slide_base(prs, "긴 일은 작업 이벤트로 넘기고, 워커는 즉시 돌아간다", kicker="장기 작업")
    lanes = [("이벤트 워커", 0.9, INK_2), ("이벤트 서버", 4.1, INK), ("작업 실행기 (별도 예산)", 7.3, TEAL), ("클라이언트 채널", 10.5, AMBER)]
    for label, x, fill in lanes:
        box(slide, I(x), I(1.75), I(2.1), I(0.6), label, fill=fill, color=PAPER if fill != AMBER else INK, size=12, bold=True)
        arrow(slide, I(x + 1.05), I(2.35), I(x + 1.05), I(5.5), color=LINE, width=1)
    steps = [
        (1.95, 5.15, 2.8, "① 착수 마킹 A → 표지 t1 (짧은 기한)", INK_2),
        (1.95, 5.15, 3.4, "② 완료 보고 (A, t1) + 후속 작업 J — 전부 또는 전무", INK_2),
        (8.35, 5.15, 4.0, "③ 착수 마킹 J → 표지 t2 (긴 기한)", TEAL),
        (8.35, 11.55, 4.6, "④ 발송용 이벤트 등록 (진행)", TEAL),
        (8.35, 11.55, 5.2, "⑤ 완료 보고 (J, t2) + 발송용 결과 이벤트", TEAL),
    ]
    for x1, x2, y, label, color in steps:
        arrow(slide, I(x1), I(y), I(x2), I(y), color=color)
        text(slide, I(min(x1, x2) + 0.1), I(y - 0.3), I(4.9), I(0.3), label, size=11, color=color, bold=True)
    box(slide, MARGIN, I(5.75), I(3.6), I(1.0), "이벤트 워커는 즉시 반환", fill=TEAL_SOFT, color=TEAL, size=14, bold=True, sub="허가는 ②에서 풀린다")
    box(slide, I(4.45), I(5.75), I(4.2), I(1.0), "실행기가 죽으면", fill=RED_SOFT, color=RED, size=14, bold=True, sub="J 기한 지남 → 회수(id 재발급, 재시도 +1) → 다른 실행기")
    text(slide, I(8.95), I(5.8), I(3.8), I(1.0), ["후속이 하나라도 무효면 부모는 착수 유지", "무효 응답 = 이번 요청은 효력 없음", "포기 여부는 재시도 카운트로 실행기가 판단"], size=11, color=MUTED, spacing=2)


def s13_fanout(prs, h):
    use(h)
    slide = slide_base(prs, "발송: 등록하는 순간 보내고 지운다", kicker="발송용 큐와 프록시")
    box(slide, I(0.8), I(2.9), I(2.4), I(1.6), "이벤트 서버", fill=INK, color=PAPER, size=16, bold=True, sub="채널 → 구독 접속")
    proxies = [2.0, 3.35, 4.7]
    for index, y in enumerate(proxies):
        box(slide, I(4.6), I(y), I(2.3), I(0.95), f"프록시 {index + 1}", fill=AMBER, size=13, bold=True, sub="송신 버퍼")
        arrow(slide, I(3.2), I(3.7), I(4.6), I(y + 0.47))
        for row in range(3):
            box(slide, I(8.3 + row * 0.75), I(y + 0.22), I(0.5), I(0.5), "", fill=INK_2, shape=MSO_SHAPE.OVAL)
        arrow(slide, I(6.9), I(y + 0.47), I(8.3), I(y + 0.47), color=LINE)
    text(slide, I(3.3), I(2.35), I(1.3), I(0.5), "버퍼에 넣고\n즉시 제거", size=11, color=MUTED)
    text(slide, I(8.1), I(5.75), I(2.6), I(0.3), "사용자별 발송은 프록시가", size=11, color=MUTED)
    items = [("구독 집약", "0→1이면 상위 구독, 반영 뒤에 사용자에게 성공. 1→0이면 취소. 새 사용자는 반영 이후 것만"),
             ("느린 접속", "서버는 기다리지 않는다. 송신 버퍼가 넘치면 연결 종료 (= 손실 신호). 순서는 연결이 보장"),
             ("재접속 (Q8)", "놓친 발송은 놓친 것. 필요하면 상태 요청 이벤트로. 프록시 짧은 버퍼는 선택")]
    for index, (title, body) in enumerate(items):
        y = I(1.95 + index * 1.45)
        box(slide, I(10.9), y, I(2.0), I(1.25), "", fill=MIST)
        text(slide, I(11.0), y + I(0.1), I(1.85), I(1.1), [(title, {"bold": True, "size": 12}), body], size=10)


def s14_auth(prs, h):
    use(h)
    slide = slide_base(prs, "인증도 이벤트로: 프록시는 로직 없는 중계기", kicker="인증·권한 (Q1 제안)")
    lanes = [("사용자", 1.0, INK_2), ("프록시", 4.0, AMBER), ("이벤트 서버", 7.0, INK), ("인증 워커", 10.0, TEAL)]
    for label, x, fill in lanes:
        box(slide, I(x), I(1.75), I(2.3), I(0.6), label, fill=fill, color=PAPER if fill != AMBER else INK, size=12, bold=True)
        arrow(slide, I(x + 1.15), I(2.35), I(x + 1.15), I(6.2), color=LINE, width=1)
    steps = [(2.15, 5.15, 2.8, "① 접속 + 토큰"), (5.15, 8.15, 3.35, "② 인증용 착수 이벤트 등록 (전용 채널)"), (11.15, 8.15, 3.9, "③ 인증 워커만 착수 마킹 가능"),
             (11.15, 8.15, 4.45, "④ 완료 보고 + 결과 발송 이벤트"), (8.15, 5.15, 5.0, "⑤ 결과 수신: 신원과 허용 범위"), (5.15, 2.15, 5.55, "⑥ 이후 호출은 범위 안에서만")]
    for x1, x2, y, label in steps:
        arrow(slide, I(x1), I(y), I(x2), I(y))
        text(slide, I(min(x1, x2) + 0.1), I(y - 0.3), I(4.8), I(0.3), label, size=11, bold=True)
    box(slide, MARGIN, I(6.4), I(12.1), I(0.5), "대안: (b) 이벤트 서버의 선언적 규칙 · (c) 프록시 JSON의 정적 토큰 — 승인 때 Q1로 정한다", fill=MIST, size=12)


def s15_guarantees(prs, h):
    use(h)
    slide = slide_base(prs, "보장 계약: 설계보다 먼저 고정한다", kicker="G1 ~ G8")
    items = [("G1", "경합하면 정확히 하나 성공", "성공한 쪽만 착수 표지를 받는다"),
             ("G2", "완료는 id + 표지 + 서비스 자격이 맞을 때만", "옛 보고와 이미 반영된 완료의 재전송은 후속과 함께 무효"),
             ("G3", "외부 효과 중복 가능", "at-least-once. 등록 건마다 고유한 작업 키로 워커가 거른다"),
             ("G4", "미처리분은 재시작을 넘는다", "Scheduled는 마지막 쓰기 이후 손실 가능"),
             ("G5", "발송은 그 순간의 구독자에게만", "송신 버퍼로 넘기는 순간 사라진다. 버퍼 포화 접속은 끊는다"),
             ("G6", "소켓을 기다리지 않는다", "한 접속이 멈춰도 다른 접속은 진행"),
             ("G7", "삭제는 대기 중일 때만", "결과 채널에 성공·실패 통지 정확히 한 건"),
             ("G8", "같은 id는 큐에 있는 동안 불가", "착수용·발송용·후속 모두. 큐를 떠난 id는 다시 쓸 수 있다")]
    width = (W - 2 * MARGIN - I(0.9)) / 4
    for index, (tag, title, body) in enumerate(items):
        col, row = index % 4, index // 4
        x = MARGIN + col * (width + I(0.3))
        y = I(1.85) + row * I(2.5)
        accent = AMBER if tag == "G3" else TEAL
        box(slide, x, y, width, I(2.2), "", fill=AMBER_SOFT if tag == "G3" else TEAL_SOFT)
        text(slide, x + I(0.2), y + I(0.15), I(1.2), I(0.6), tag, size=28, bold=True, color=accent)
        text(slide, x + I(0.2), y + I(0.8), width - I(0.4), I(0.6), title, size=14, bold=True)
        text(slide, x + I(0.2), y + I(1.45), width - I(0.4), I(0.7), body, size=11, color=MUTED)
    footer(slide, "각 단계의 인수 테스트가 이 계약을 판정한다")


def s16_journal(prs, h):
    use(h)
    slide = slide_base(prs, "저널링은 메서드가 아니다", kicker="재기동 복구가 최우선")
    width = (W - 2 * MARGIN - I(0.4)) / 2
    cols = [("재기동 복구 (최우선)", TEAL, TEAL_SOFT, ["대상: 미처리 착수용 엔벨롭(착수 블록 내용은 빼고 착수 여부만), 세대, 이름 정의", "새 세대를 내구화한 뒤 첫 응답 → 옛 착수 표지 전부 무효", "착수 중이던 것은 대기로, id 재발급 (카운트는 Q7)", "대기 순서는 작업 키 순서 (UUID 정렬 아님)", "쓰기 실패는 서버 정지 · Strict는 기록 뒤에 통지"]),
            ("운영 스냅샷", AMBER, AMBER_SOFT, ["대상: 설정에서 고른 채널·이벤트 (발송용 포함)", "이벤트 발생 시점에 수집, 별도 파일에 단위별로", "재기동 때 다시 발송하지 않음", "쓰기 실패해도 서버는 계속, 정상 종료 때 남은 부분 기록"])]
    for index, (title, accent, fill, lines) in enumerate(cols):
        x = MARGIN + index * (width + I(0.4))
        box(slide, x, I(1.85), width, I(0.7), title, fill=accent, color=PAPER if accent == TEAL else INK, size=18, bold=True)
        box(slide, x, I(2.65), width, I(3.1), "", fill=fill)
        text(slide, x + I(0.35), I(2.9), width - I(0.7), I(2.8), [(line, {"bullet": True, "after": 10}) for line in lines], size=14)
    box(slide, MARGIN, I(6.0), I(12.1), I(0.7), "유지: 순변화 기록 · 쓰기 비중첩 · 사슬 · 압축 · 파일 머리. 형식 v2 (등록 · 착수 · 제거 · 회수 · 세대 · 이름 정의)", fill=MIST, size=13, bold=True)


def s17_hotpath(prs, h):
    use(h)
    slide = slide_base(prs, "핫패스: 모델이 덜어 내고, 나머지는 측정한 뒤 바꾼다", kicker="이벤트당 비용")
    rows = [["#", "지금", "바꿀 것", "분류"],
            ["1·2·3", "로그 두 행, 메시지·실행 표 이중, begin마다 payload 해시", "보존 로그 없음 · 이벤트 하나 · id 조회 한 번", "모델 변경"],
            ["7", "dequeue와 begin 왕복", "착수 마킹 한 번", "모델 변경"],
            ["4", "배치마다 모든 큐·대기자 순회, 기한 최솟값 재계산", "바뀐 큐만 · 기한 힙", "P0 측정 → P1"],
            ["5", "잠금 중 이벤트마다 구독자 깨우기", "채널당 배치당 한 번, 잠금 밖", "P0 측정 → P1"],
            ["6", "실행당 태스크 둘", "future 단위 panic 포착", "P0 측정 → P1"],
            ["8", "응답마다 클로저 박싱", "응답 목록을 값으로 순회", "P0 측정 → P1"]]
    table(slide, MARGIN, I(1.85), I(12.1), rows, [0.8, 5.0, 3.6, 1.5], size=12, row_h=0.55)
    text(slide, MARGIN, I(6.0), I(12), I(0.6), "이벤트 한 건의 비용이 서버 전체의 비용이다. 서버 정책과 보존 로그가 빠지면서 이벤트당 일이 크게 준다.", size=12, color=MUTED)


def s18_plan(prs, h):
    use(h)
    slide = slide_base(prs, "단계 계획: 측정부터, 단계마다 증거로 닫는다", kicker="P0 ~ P6")
    phases = [("P0", "현행 기준선", "Scheduled·Strict·휘발 CPU·할당·p99. UUIDv7 생성 비용"),
              ("P1", "깨지지 않는 개선", "저널 실패로 run이 실패 종료. 깨우기 누락 결정적 시험"),
              ("P2", "엔진 재작성", "선행 Q3~Q7·Q9~Q11. 단일 엔벨롭, 두 큐, 메서드 일곱, 표지·작업 키, 저널 v2"),
              ("P3", "워커를 메서드 위로", "선행 Q2. 재시도 정책을 워커로, 작업 실행기, 과부하 판정"),
              ("P4", "원격 프로토콜", "로컬=원격. 워커 종료 → 회수 → 새 id. 서버만 재기동 뒤 옛 보고 무효"),
              ("P5", "프록시 바이너리", "선행 Q1·Q8. 사용자↑·프록시 고정과 반대 측정"),
              ("P6", "apibondrust", "외부 API 실패가 실패 이벤트 하나로")]
    width = (W - 2 * MARGIN - I(0.6)) / 7
    for index, (tag, title, proof) in enumerate(phases):
        x = MARGIN + index * (width + I(0.1))
        box(slide, x, I(1.9), width, I(0.9), tag, fill=INK if index else AMBER, color=PAPER if index else INK, size=20, bold=True, shape=MSO_SHAPE.CHEVRON if index else MSO_SHAPE.PENTAGON)
        text(slide, x + I(0.05), I(3.0), width - I(0.1), I(0.7), title, size=14, bold=True)
        text(slide, x + I(0.05), I(3.75), width - I(0.1), I(2.3), proof, size=11, color=MUTED)
    box(slide, MARGIN, I(6.15), I(12.1), I(0.6), "각 단계의 끝: cargo xtask verify · 원문 인용 인수 테스트 · G1~G8 판정 · 문서 · 커밋", fill=TEAL_SOFT, color=TEAL, size=13, bold=True)


def s19_risks(prs, h):
    use(h)
    slide = slide_base(prs, "위험과 대가", kicker="알고 승인할 것")
    rows = [["위험", "내용", "대응"],
            ["API 파괴", "엔진 API 전체가 일곱 메서드로 바뀐다", "0.x. API 고정본 차이를 보고"],
            ["발송 유실", "끊겨 있던 클라이언트는 그동안의 발송을 받지 못한다", "원문이 정한 성질. Q8"],
            ["완료 뒤 재전송", "같은 id 거절은 큐에 있는 동안만", "원문(영원히 고유할 필요 없음). 필요하면 워커가 거름"],
            ["착수 경합 비용", "착수 가능 알림이 구독 워커 수만큼, 대부분 착수 실패", "Q3. P2에서 측정"],
            ["회수 지연", "등록이 끊긴 동안 기한 지난 착수가 회수되지 않음", "원문 방식의 성질"],
            ["외부 효과 중복", "회수 뒤 재실행", "G3. 작업 키를 워커에게"],
            ["단일 엔진 천장", "큐 연산은 한 코어", "이벤트당 비용 절감이 1차"],
            ["저널 형식 변경", "v1을 읽지 못한다", "운영 데이터 없는 단계"],
            ["수신 버퍼 보존", "작은 payload 조각이 큰 수신 버퍼를 붙잡음", "분리 복사 안을 P0·P2에서 측정"]]
    table(slide, MARGIN, I(1.8), I(12.1), rows, [1.8, 4.6, 3.8], size=12, row_h=0.5)


def s20_decisions(prs, h):
    use(h)
    slide = slide_base(prs, "승인 때 정할 것: 미결정 열한 가지", kicker="Q1 ~ Q11")
    items = [("Q1", "최종 사용자 인증", "인증용 착수 이벤트", "선언적 규칙 · 정적 토큰"),
             ("Q2", "워커 기본 기한", "생략하면 30초", "생략하면 구성 오류"),
             ("Q3", "착수 대상을 아는 법", "참조 알림 + 경합", "여유 알린 워커 하나씩"),
             ("Q4", "완료에 후속 싣기", "실어서 한 스텝", "따로 부름"),
             ("Q5", "등록자가 id 지정", "줄 수 있음, 없으면 서버", "항상 서버"),
             ("Q6", "끊긴 워커의 착수", "측정 후 결정", "즉시 회수 / 기한까지"),
             ("Q7", "재기동 때 재시도", "+1", "그대로"),
             ("Q8", "재접속 처리", "놓침 허용 + 상태 요청", "프록시 짧은 버퍼"),
             ("Q9", "v1 저널 이전", "하지 않음", "변환 도구"),
             ("Q10", "늦은 완료의 마감", "실제 회수가 마감", "기한 자체가 마감"),
             ("Q11", "principal의 뜻", "서비스 자격, 위임 별도", "하나로 겸함")]
    width = (W - 2 * MARGIN - I(0.9)) / 4
    for index, (tag, title, pick, other) in enumerate(items):
        col, row = index % 4, index // 4
        x = MARGIN + col * (width + I(0.3))
        y = I(1.8) + row * I(1.65)
        box(slide, x, y, width, I(1.5), "", fill=MIST)
        chip(slide, x + I(0.15), y + I(0.12), tag, fill=INK, color=PAPER)
        text(slide, x + I(1.0), y + I(0.12), width - I(1.1), I(0.4), title, size=13, bold=True)
        text(slide, x + I(0.15), y + I(0.6), width - I(0.3), I(0.85), [("제안: " + pick, {"bold": True, "color": TEAL}), ("대안: " + other, {"color": MUTED})], size=11)
    footer(slide, "Q3~Q7·Q10·Q11은 원문에 없는 부분을 메운 해석 · P2 전 Q3~Q7·Q9~Q11, P3 전 Q2, P5 전 Q1·Q8")


def s21_review(prs, h):
    use(h)
    slide = slide_base(prs, "외부 리뷰", kicker="Codex gpt-6.2-astra · high")
    from review_numbers import ROUNDS

    rows = [["회차", "치명", "높음", "중간", "사실", "핵심 지적과 반영"]]
    for round_ in ROUNDS:
        counts = [str(value) for _, value, _ in round_["stats"]]
        rows.append([round_["title"]] + counts + [" · ".join(round_["lines"])])
    table(slide, MARGIN, I(1.85), I(12.1), rows, [2.6, 0.6, 0.6, 0.6, 0.6, 7.1], size=11, row_h=0.82)
    footer(slide, "전문: target/codex-review/r1 ~ r" + str(len(ROUNDS)) + " 의 report.json · 반영 기록: 제안서 12장")


def s22_ask(prs, h):
    use(h)
    slide = slide_base(prs, "승인을 요청합니다", kicker="다음 단계", dark=True)
    text(slide, MARGIN, I(1.9), I(7.4), I(3.2), [("승인하면", {"bold": True, "color": AMBER, "size": 18}),
                                                 ("P0 측정부터 순서대로 진행", {"bullet": True}),
                                                 ("단계마다 측정 · 테스트 · 차이를 보고", {"bullet": True}),
                                                 ("P2 전 Q3~Q7·Q9~Q11, P3 전 Q2, P5 전 Q1·Q8", {"bullet": True}),
                                                 ("원문과 충돌하는 판단이 생기면 멈추고 묻는다", {"bullet": True})], size=18, color=PAPER, spacing=10)
    box(slide, I(8.6), I(1.9), I(4.1), I(3.6), "", fill=INK_2)
    text(slide, I(8.85), I(2.1), I(3.6), I(3.3), [("함께 보는 문서", {"bold": True, "color": AMBER}), "docs/proposals/", "event-server-redesign.md", "", ("이 덱", {"bold": True, "color": AMBER}), "event-server-redesign.pptx", "(생성기: deck/build.py)"], size=14, color=PAPER)
    footer(slide, "승인 전에는 코드를 바꾸지 않는다", dark=True)


SLIDES = [s01_title, s02_summary, s03_now, s04_diagnosis, s05_principles, s06_target, s07_methods, s08_lifecycle, s09_expiry, s10_two_queues, s10c_envelope, s11_ids, s12_handoff, s13_fanout, s14_auth, s15_guarantees, s16_journal, s17_hotpath, s18_plan, s19_risks, s20_decisions, s21_review, s22_ask]
