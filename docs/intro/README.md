# 소개 덱

`ruvent-intro.pptx`는 ruvent를 처음 보는 개발자용 소개 자료다(18장). 코드에서 만든다.

```sh
python docs/intro/build.py                         # docs/intro/ruvent-intro.pptx
python docs/intro/build.py target/intro-draft.pptx # 덱을 PowerPoint로 열어 둔 동안
powershell -NoProfile -ExecutionPolicy Bypass -File docs/proposals/deck/render.ps1 -Deck docs/intro/ruvent-intro.pptx -Out target/intro-render
```

`render.ps1`은 PowerPoint로 장마다 PNG를 뽑는다. 고친 뒤에는 모든 장을 열어 글 넘침과 줄바꿈을 본다.

## 이력

| 판 | 받은 평 | 고친 것 |
| --- | --- | --- |
| 1 (2026-11-27) | "AI 냄새가 많이 나는 ppt" | 모양: 표지 글자, 구호형 제목, 번호 원, 같은 카드 격자, 큰 숫자 강조, 장식 도형 제거 |
| 2 | "말투나 어말어미, 명사나 무리한 현재분사의 한국어화 등 흔적이 많이 남아 있음" | 글: 아래 글 규칙으로 전 문장 재작성 |
| 3 | | 현재 판 |

## 글 규칙

사용자의 강의 장표 규칙(`~/.claude/skills/lecture-slides/references/writing.md`)을 따른다. 검사기도 같이 쓴다.

```sh
# 덱의 글을 뽑아 === SLIDE n === 머리로 나눈 txt를 만든 뒤
node ~/.claude/skills/lecture-slides/scripts/lint_text.js target/intro-text.txt
```

| 하지 않음 | 대신 | 예 |
| --- | --- | --- |
| 서술형 종결 `~다.` | 명사 · 명사형(함 · 됨 · 없음) | 재기동을 넘는다 → 재시작 후 복구 |
| 문장형 제목 | 제목은 명사구, 논지는 `핵심:` 한 줄 | 엔진은 태스크 하나가 장부를 쥐는 단일 작성자 구조다 → 엔진 내부 구조 / 핵심: 상태를 고치는 태스크는 하나뿐(single writer) |
| 코드 개념을 우리말로 지어 부르기 | 실제 이름을 쓰고 처음 한 번 풀이 | 관문 → 검사 단계, 흘려 읽은 본문 → 디스크로 받아 둔 본문, 기한 휠 → 타이밍 휠, 창 → window |
| `-하는 / -되는` 관형절 쌓기 | 문장을 나누거나 명사구로 | 서버가 그 값으로 행동하는 필드 → 서버 동작에 쓰는 필드 |
| 이중 피동 · `~를 통해` · `~에 대해` · `~을 가지다` | 능동 · 조사로 | |
| `A가 아니라 B` 대구 | 사실만 | lease는 엔벨롭이 아니라 착수 기록에 있다 → lease 기한은 엔벨롭 밖, 서버의 착수 기록에 보관 |
| 콜론 소제목, 굵은 글씨 | `1.소제목` + 하위 줄, 보통 굵기 | |

## 모양 규칙

| 하지 않음 | 대신 |
| --- | --- |
| 제목 위의 작은 표지 글자, 구호형 제목 | 명사구 제목 + `핵심:` 한 줄 |
| 같은 크기 카드 격자 | 가는 가로줄 표, 2단 글, 도해 |
| 큰 숫자 강조 | 실제 출력 줄(보고서 파일에서 옮김) |
| 장식 도형, 어두운 표지 · 끝장 | 흰 바탕 |
| 색 여러 개 | 녹 = 착수용, 청록 = 발송. 미해결 문제 표시에만 녹을 더 씀 |
| 고맙습니다 · 질문 장 | 처음 할 일 |

숫자는 옆에 적힌 파일에서 옮긴다. 다시 재지 않은 측정은 그렇다고 적는다.

## 출처

* [daleseo/korean-skills humanizer](https://www.skills.sh/daleseo/korean-skills/humanizer): 번역투(`~에 대해`, `~를 통해`, `~에 있어`, `가지다`, 이중 피동), 같은 어미 반복
* [NomaDamas/k-skill korean-humanizer](https://vibehackers.io/claude-code/skills/korean-humanizer): 관형절 좌향 수식, `A가 아니라 B` 대구, 명사화 과잉, 콜론 헤딩
* [AI가 쓴 글, 한국인은 왜 몇 줄 만에 알아챌까(브런치)](https://brunch.co.kr/@ainote/147): 어미와 문장 구조에서 드러나는 티
* [Laith0003/ux-skill: Anti AI slop ban list](https://github.com/Laith0003/ux-skill/wiki/Anti-AI-slop-ban-list.md): 같은 카드 격자, 장식 도형, 구호형 제목
* [Superdots, DEV](https://dev.to/superdots/how-to-make-ai-presentations-that-dont-look-ai-generated-bhk): 개요 반복 장 · 고맙습니다 장 빼기, 숫자 원본 대조
