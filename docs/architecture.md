# 구조

## 배치

```text
일반 클라이언트 ──► 프록시(ruvent-proxy로 빌드) ──────┐  채널별 상위 구독 하나
                                                     ├──► ruvent-server ──► Engine (단일 작성자)
워커 서버(ruvent-client: App) ───────────────────────┘          │
내장 워커: ruvent::connect — 같은 메서드, 소켓 없음 ────────────┘
```

서버는 접속자가 워커인지 프록시인지 묻지 않는다. 모든 접속은 서비스 자격(토큰 → 이름)으로 열리고, 권한은
자격별 `Grant`(메서드, 채널 패턴, 위임 허용)로 판정한다. 연결 수는 "프록시 수 + 워커 서버 수"다.

## 엔진 ([crates/ruvent/src/engine](../crates/ruvent/src/engine/mod.rs))

```text
접속 ──Op──► inbox ──recv_many──► State(상태 기계) ──Effects──┬─► 송신 버퍼(try_send, 가득 차면 연결 종료)
                                                               ├─► 저널 기록 태스크(한 번에 한 비행)
                                                               └─► 스냅샷 수집기(try_send, 넘치면 셈)
```

* `State`([state/mod.rs](../crates/ruvent/src/engine/state/mod.rs))는 순수 상태 기계다. 호출을 받아
  `Effects`(저널 레코드, 송신, 스냅샷 사본)를 내놓는다. 태스크도 소켓도 시계도 없다. 엔진이 시각을 넣어 준다.
* 착수용 큐는 작업 키 순서의 `BTreeMap`이다. id 색인, 채널별 대기 집합(대기분 응답용), 기한 힙
  `(기한, 작업 키, 표지)`을 함께 둔다. 힙에서 꺼낸 항목은 아직 같은 표지로 착수 중일 때만 회수한다.
* 엔진은 소켓도 타이머도 기다리지 않는다. 저널 쓰기는 한 비행씩 기록 태스크에 넘긴다. 비행 중에 생긴
  레코드는 다음 비행에 실린다.
* **Strict 보류.** 착수용 등록·완료(후속 포함)·삭제 성공의 응답과 통지는 그 배치의 레코드가 실린 비행이
  끝난 뒤에 넘긴다. 보류 중인 송신이 있는 접속에는 그 뒤에 처리된 것도 뒤에 선다(접속별 순서). 착수·회수·
  구독·직접 발송은 보류하지 않는다. 세대가 바뀐 배치는 내구성 설정과 무관하게 전부 보류한다.
* **장애.** 저널 쓰기가 실패하면 엔진은 멈추고 `Engine::stopped()`가 사유와 함께 끝난다. 보류 중이던 응답과
  통지는 버려진다. 엔진은 정지를 알리기 전에 저널 기록 태스크를 끝내 파일 잠금을 놓는다.

## 엔벨롭 ([crates/ruvent/src/envelope](../crates/ruvent/src/envelope/mod.rs))

```text
 고정 머리 27   id 16 · channel 4 · type 4 · flags 1 · retry 2
 작업 키 8      generation 4 · arrival 4                      (착수용, 서버가 보낼 때)
 착수 블록 24   token 8 · claimer 4 · worker 4 · deadline 8    (착수 응답)
 선택 필드      reply 4 · correlation 16 · principal 4 · delegate 4
 payload        길이 4 + 바이트
```

* `flags`: 비트 0 종류(1 = 발송용), 1 착수 여부, 2 reply, 3 correlation, 4 principal, 5 delegate, 6~7 예약.
* 크기: 발송용 31, 대기 중 착수용 39, 착수 중 착수용 63바이트(각각 payload 별도, 선택 필드 없음).
* 레코드 종류별로 실리는 필드는 `Layout`이 정한다(Request, Notice, Claimed, Delivery, Journal). 디코더는
  레코드 종류와 `flags`만으로 길이를 안다. 발송용과 착수의 조합, 예약 비트, 남는 바이트는 거절한다.
* 제자리 변경은 엔진의 상주 사본에만 한다. 송신·저널·수집기에 넘길 때는 머리 값을 복사하고, 이름 핸들과
  payload는 공유한다(`Envelope::clone`).
* 세대는 서버 전체에 하나인 단조 카운터다. 기동할 때, 그리고 도착·착수 순번이 u32를 다 쓰면 올라간다. 새
  세대는 저널에 기록된 뒤에 쓰인다.

## 이름표

세 공간으로 나뉜다.

* 서버 안: 공유 핸들 `Name`, 사전 `Names`.
* 접속마다: 프로토콜의 방향별 사전. 상한을 넘으면 초기화 레코드를 쓴다.
* 파일마다: 저널 파일·스냅샷 파일이 각자 이름을 정의하므로 파일 하나만으로 해독된다.

## 저널 v2 ([crates/ruvent/src/journal](../crates/ruvent/src/journal/mod.rs))

```text
"ruvent\0" 0x02
[len u32][crc32 u32] [kind u8][prev u64][through u64][count u32][record…]
record: 1 Register(journal 레이아웃 본문) · 2 Claim(키) · 3 Remove(키) · 4 Reclaim(키, id, retry) · 5 Generation · 6 Name
```

* 레코드는 작업 키로 등록 건을 가리키므로 재생에 id 조회가 필요 없다.
* 한 연산의 순변화(완료의 부모 제거와 후속 등록 전부)는 한 배치 프레임에 들어간다. 복구는 완전한 프레임까지만
  재생하므로, 쓰기가 끊긴 결과는 "부모 유지·후속 0건"과 "부모 제거·후속 전부" 둘 중 하나다.
* 찢어진 마지막 프레임은 잘라 내고 앞부분을 복구한다. 형식이 다른 파일(v1 포함)은 `InvalidData`로 거부하고
  건드리지 않는다.
* 압축: 파일의 레코드가 `min_records`를 넘고 생존분의 `ratio`배를 넘으면 현재 상태 스냅샷으로 다시 쓴다
  (임시 파일 + 원자적 이름 바꾸기).
* 재기동: 새 세대를 저널에 쓴 뒤에 첫 응답을 낸다. 착수 중이던 것은 대기로 되돌리고 id를 재발급하며 재시도
  +1을 준다(Q7 (a)). 세대가 바뀌므로 재기동 전의 착수 표지는 모두 무효다.

## 운영 스냅샷 ([crates/ruvent/src/snapshot](../crates/ruvent/src/snapshot/mod.rs))

고른 채널·종류의 이벤트(착수용·발송용)를 등록되는 순간 수집기에 복사한다. 수집기는 개수나 나이 중 먼저
닿는 단위로 파일 하나(`snapshot-<ms>-<seq>.rvs`)를 쓴다. 재기동 때 재생하지 않는다. 쓰기 실패는 세고
서버는 계속 돈다. 정상 종료 때 남은 단위를 쓴다.

## 와이어 ([crates/ruvent-protocol](../crates/ruvent-protocol/README.md))

핸드셰이크(Hello/Welcome/Bye), 프레임 `[len][count][records]`, 메서드 요청·응답·알림·전달·이름 정의·사전
초기화 레코드로 이뤄진다. 엔벨롭 본문은 코어 코덱 하나로 인코딩한다.

## 메서드 계약 ([crates/ruvent/src/methods](../crates/ruvent/src/methods/mod.rs))

* `Methods` 트레이트 하나에 구현은 `Client` 하나뿐이다. 링크(`Link`)가 둘이다. 인프로세스 링크는 엔진
  연산함으로 보내고(`ruvent::connect`), 소켓 링크는 쓰기 태스크로 보낸다(`ruvent_client::remote::connect`).
* 서버가 보낸 것은 도착 순서대로 `Demux::route`가 나눈다. 응답은 호출자에게, 알림·전달은 `Inbox`로 간다.
  구독 응답은 `Incoming::Subscribed` 표지를 inbox에 먼저 넣고 나서 호출자에게 간다. 그래서 프록시는 그 표지
  이전의 이벤트가 구독 전에 보내진 것임을 안다.

## 워커와 프록시

* 워커 프레임워크([crates/ruvent-client](../crates/ruvent-client/README.md)): 서비스마다 lane 태스크 하나가
  허가, 보관 알림, 재조회를 소유한다. 실행은 착수 → 핸들러 → 완료다. 허가는 핸들러가 끝나면 돌아온다.
* 프록시([crates/ruvent-proxy](../crates/ruvent-proxy/README.md)): 채널별 상위 구독 하나를 두고 사용자에게
  나눠 보낸다. 사용자가 붙을 때마다 상위 구독을 한 번 더 보내고, 그 표지 이후의 이벤트만 그 사용자에게
  보낸다. 서버는 같은 접속의 중복 구독을 하나로 둔다. 프록시는 완성품 바이너리가 아니라 라이브러리다.
  프로토콜은 feature로 고르고, 인증·구독·등록·전달은 훅(`Hooks`)으로 바꾼다. 훅을 쓰지 않으면 JSON 설정대로
  돈다. 전달 훅만 허브의 단일 태스크에서 동기로 불리고, 나머지는 사용자 태스크에서 돈다.
