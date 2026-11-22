"""Counts shown on the review slide, taken from the review reports.

r1: target/codex-review/r1/result.md — design findings 1–12 by stated severity
    (치명 2·5·6·10, 높음 1·3·4·7·8·9·12, 중간 11), and fact claims judged other
    than "정확" (D6, D8, D9; 1장 엔진·전송·측정; D9-1, D9-11) = 8.
r2: target/codex-review/r2/result.md — new findings 1–12 by stated severity
    (치명 1·2, 높음 3·4·5·6·8·9·10·12, 중간 7·11); r1 findings judged
    해소 5 / 부분 해소 11 / 미해소 0; fact corrections D5, D8 = 2.
"""

from build import AMBER, INK, MUTED, RED, TEAL

ROUNDS = [
    {
        "title": "1회차 — 승인 보류",
        "stats": [("치명", 4, RED), ("높음", 7, AMBER), ("중간", 1, MUTED), ("사실 정정", 8, INK)],
        "lines": [
            "보장 계약 G1~G7을 먼저 고정",
            "착수 번호를 접속 세대에 묶고 낡은 자식까지 무효",
            "작업 인계를 레코드로, 저널 v2 복구 동치",
            "인증 결과의 발급 권한과 위임 근거",
        ],
    },
    {
        "title": "2회차 — 새 지적과 1회차 해소 판정",
        "stats": [("치명", 2, RED), ("높음", 8, AMBER), ("중간", 2, MUTED), ("사실 정정", 2, INK)],
        "lines": [
            "1회차 16건: 해소 5 · 부분 해소 11 · 미해소 0",
            "완료 거절 ≠ 완료 확인 → complete 판정 다섯 가지",
            "인계는 전부 또는 전무, 포화는 작업 큐 상한",
            "시도 횟수 = 승인된 착수 횟수, 지연은 재시작 뒤에도 유지",
            "gap 단독 전달, 관심 집합 복원, 리액션 세대 비교",
        ],
    },
]
