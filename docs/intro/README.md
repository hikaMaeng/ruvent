# 소개 덱

`ruvent-intro.pptx`는 ruvent를 처음 보는 개발자를 위한 소개 자료다(18장). 코드에서 만든다.

```sh
python docs/intro/build.py                         # docs/intro/ruvent-intro.pptx
python docs/intro/build.py target/intro-draft.pptx # 덱을 PowerPoint로 열어 둔 동안
powershell -NoProfile -ExecutionPolicy Bypass -File docs/proposals/deck/render.ps1 -Deck docs/intro/ruvent-intro.pptx -Out target/intro-render
```

`render.ps1`은 PowerPoint로 장마다 PNG를 뽑는다. 고친 뒤에는 모든 장을 열어 글 넘침과 줄바꿈을 본다.

## 모양 규칙

첫 판(2026-11-27)은 "AI가 만든 티가 난다"는 평을 받았다. 아래 규칙은 그 판을 다시 짤 때 정했다. 근거는 맨 아래 출처다.

| 하지 않는다 | 대신 |
| --- | --- |
| 제목 위의 작은 표지 글자(eyebrow) | 제목 하나. 그 장이 말하는 바를 문장으로 쓴다 |
| 구호형 제목("무엇으로 믿는가", "다음 걸음") | 내용을 담은 제목("마지막 cargo xtask verify에서 관문 11개가 모두 통과했다") |
| 순서가 없는 항목의 번호 원 | 번호는 순서가 있을 때만(시퀀스, 처음 할 일) |
| 크기가 같은 카드 세 개·여섯 개 격자 | 표(가는 가로줄), 2단 글, 도해 |
| 큰 숫자 강조(11, 264, 68, 7/7) | 실제 출력 줄을 그대로 보인다(보고서 파일에서 옮김) |
| 장식용 도형(표지의 원), 어두운 표지·끝장 | 흰 바탕, 검은 글자 |
| 색 여러 개 | 의미 있는 색 둘만: 녹 = 착수용, 청록 = 발송용. 열린 문제 표시에만 녹을 더 쓴다 |
| 모든 장의 꼬리글 | 쪽 번호만 |
| 고맙습니다·질문 장 | 처음 하루에 할 일(명령과 문서 순서) |

숫자는 옆에 적힌 파일에서 옮긴다. 다시 재지 않은 측정은 그렇다고 적는다.

## 출처

* [Laith0003/ux-skill: Anti AI slop ban list](https://github.com/Laith0003/ux-skill/wiki/Anti-AI-slop-ban-list.md): 같은 카드 세 개, 장식 도형, 모든 요소의 같은 그림자, 구호형 제목, 순서 없는 번호 목록
* [How to make AI presentations that don't look AI-generated (Superdots, DEV)](https://dev.to/superdots/how-to-make-ai-presentations-that-dont-look-ai-generated-bhk): 결론을 담은 제목, 개요 반복 장과 고맙습니다 장 빼기, 꾸미기만 하는 그림 빼기, 숫자 원본 대조
* [skills-slides (DEV)](https://dev.to/nghiahsgs/i-built-a-claude-code-skill-that-generates-50000-unique-slide-designs-and-killed-the-ai-slop-3g4c): 정해진 토큰(색·글꼴·배치)에서 고르고 금지 목록으로 검사
