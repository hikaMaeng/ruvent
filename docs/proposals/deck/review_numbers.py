"""Counts shown on the review slide, taken from the review reports.

r1: target/codex-review/r1/result.md — design findings 1–12 by stated severity
    (치명 2·5·6·10, 높음 1·3·4·7·8·9·12, 중간 11), and fact claims judged other
    than "정확" (D6, D8, D9; 1장 엔진·전송·측정; D9-1, D9-11) = 8.
r2: target/codex-review/r2/result.md — new findings 1–12 by stated severity
    (치명 1·2, 높음 3·4·5·6·8·9·10·12, 중간 7·11); r1 findings judged
    해소 5 / 부분 해소 11 / 미해소 0; fact corrections D5, D8 = 2.
r3: target/codex-review/r3/result.md — 치명 3, 높음 5, 중간 2; fact corrections
    D6, D9, D11, 6.9의 3·8 = 5 (D12 was a scope-narrowing recommendation).
r5: target/codex-review/r5/result.md — r4 rows judged 해소 3 / 부분 해소 10 /
    미해소 0; new findings 1–10 by stated severity (치명 1·2, 높음 3·4·5·6·7·8,
    중간 9·10); fact corrections 6.0 대응표 audience·created_at = 2.
r4: target/codex-review/r4/result.md — r3 rows judged 해소 6 / 부분 해소 6 /
    미해소 0; new findings 1–11 by stated severity (치명 1·2, 높음 3·4·5·6·7·8,
    중간 9·10·11); fact corrections D11 영향 열, D14, 6.0 대응표 5행 = 7.
"""

from build import AMBER, INK, MUTED, RED, TEAL

ROUNDS = [
    {
        "title": "1회차 — 승인 보류",
        "stats": [("치명", 4, RED), ("높음", 7, AMBER), ("중간", 1, MUTED), ("사실", 8, INK)],
        "lines": [
            "보장 계약을 먼저 고정",
            "착수 번호를 접속 세대에 묶기",
            "작업 인계를 레코드로",
            "인증 결과의 발급 권한",
        ],
    },
    {
        "title": "2회차 — 1회차 해소 판정",
        "stats": [("치명", 2, RED), ("높음", 8, AMBER), ("중간", 2, MUTED), ("사실", 2, INK)],
        "lines": [
            "1회차 16건: 해소 5 · 부분 11",
            "인계는 전부 또는 전무",
            "시도 횟수 = 승인된 착수 수",
            "gap 단독 전달, 관심 집합 복원",
        ],
    },
    {
        "title": "3회차 — 두 큐 · 일곱 메서드",
        "stats": [("치명", 3, RED), ("높음", 5, AMBER), ("중간", 2, MUTED), ("사실", 5, INK)],
        "lines": [
            "승자·완료 권한 → 착수 표지",
            "후속 거절 ≠ 응답 유실",
            "서버만 재기동 → 세대",
            "알림 누락 → 구독 때 대기분",
        ],
    },
    {
        "title": "4회차 — 첫 엔벨롭",
        "stats": [("치명", 2, RED), ("높음", 6, AMBER), ("중간", 3, MUTED), ("사실", 7, INK)],
        "lines": [
            "3회차 12건: 해소 6 · 부분 6",
            "작업 키 충돌 → 등록 건 단위 키",
            "번호표 수명 → 세 공간",
            "Strict 공개 경계, 대기분 이어받기",
        ],
    },
    {
        "title": "5회차 — 단일 엔벨롭",
        "stats": [("치명", 2, RED), ("높음", 6, AMBER), ("중간", 2, MUTED), ("사실", 2, INK)],
        "lines": [
            "4회차 13건: 해소 3 · 부분 10",
            "제자리 변경 → 인계 때 머리 복사",
            "완료+후속 → 한 배치 프레임",
            "기한 u64 · 세대 u32 · 귀속 번호",
        ],
    },
]
