# 구조

## 배치

```text
일반 클라이언트 ──► 프록시(ruvent-proxy로 빌드) ──────┐  서버마다, 대상마다 상위 구독 하나
                                                     ├──► ruvent-server ──► Engine (단일 작성자)
워커 서버(ruvent-client: App) ───────────────────────┘          │
내장 워커: ruvent::connect — 같은 메서드, 소켓 없음 ────────────┘
```

서버는 접속자가 워커인지 프록시인지 묻지 않는다. 모든 접속은 서비스 자격(토큰 → 이름)으로 열리고, 권한은
자격별 `Grant`(메서드, 채널 패턴, 위임 허용)로 판정한다. 연결 수는 "프록시 수 + 워커 서버 수"다.

## 엔진 ([crates/ruvent/src/engine](../crates/ruvent/src/engine/mod.rs))

개편 계획 [library-overhaul.md](proposals/library-overhaul.md) v4.6의 장부다. 큐 구조는 계획 2.2의 (가)다:
tokio 태스크 하나가 장부를 쥐고 `mpsc` + `recv_many`로 연산을 받는다.

```text
연결(역할: 생산·작업·관찰) ──Call──► inbox ──recv_many──► State(단일 작성자) ──Effects──┬─► 연결별 송신 대기열
                                                                                       ├─► 로그 작성기(한 번에 한 비행)
                                                                                       ├─► 본문 읽기(콜드 송신, 미리 읽기)
                                                                                       ├─► 체크포인트 작성기(훑기 조각)
                                                                                       ├─► 잡일(이주, 세그먼트 삭제)
                                                                                       └─► 스냅샷 수집기
                                                   기한 휠 ──(가장 이른 기한만)──► State
```

* `State`([state/mod.rs](../crates/ruvent/src/engine/state/mod.rs))는 순수 상태 기계다. 시각을 입력으로
  받고 태스크·소켓·시계가 없다. 부분은 폴더로 나뉜다: 생산(`produce`, `producers`), 소비(`consume`, `lanes`,
  `wheel`), 정체(`pause`, `roots`), 디스크(`disk`, `checkpoint`), 복구(`recover`), 관찰(`observe`), 슬롯(`slots`).
* **식별자.** 논리 id는 회수해도 바뀌지 않는다. 시도는 착수 표지(`ClaimToken`, u64 순번)로 가른다. `WorkKey`
  (세대, u64 도착 순번)가 대기 순서다. 세대는 기동 때만 오른다. 대기 줄은 (채널, 타입)별이다.
* **생산.** 생산자 순번표가 재전송을 조용히 버리고, 연산별 기준(영속 레코드는 fsync, 그 밖은 적용)으로
  누적 `Committed`를 낸다. 계기는 로그 쓰기 완료와 장부 묶음 끝이다. 타이머는 없다.
* **소비.** 작업 연결이 `Join`하고 절댓값 창(`Window`)을 연다. 장부는 자리가 남은 참여자를 등록순으로
  돌아가며 배정한다(`Assign`). lease는 배정한 순간부터 재고, 회수하면 그 참여의 창을 닫는다. `Extend`,
  `Release{지연, 실패}`, `not_before`, `expires`, 격리(`isolate_after`), `serialize_group`이 있다.
* **커밋 가시성.** Strict에서 영속 이벤트는 등록이 커밋되기 전에는 배정·통지·backlog·명시 착수 어느 경로로도
  보이지 않는다. 같은 연결의 뒤 송신은 앞 송신의 커밋과 본문 읽기를 기다린다.
* **자원.** 관리 메모리와 디스크를 바이트로 회계하고 뿌리 채널(계보의 첫 채널)마다 몫을 센다. 고수위에서
  몫이 가장 큰 뿌리를 정지시킨다: 생산 연결에 `Paused`, 순증 줄의 배정 정지.
* **시각.** 누적 가동 시각(ms)이다. 로그의 마지막 묶음에서 이어 가고 정지 시간은 세지 않는다.
* **장애.** 로그 쓰기, 체크포인트 쓰기, 본문 읽기(CRC 불일치 포함)가 실패하면 엔진은 멈추고
  `Engine::stopped()`가 사유와 함께 끝난다.

## 엔벨롭 ([crates/ruvent/src/envelope](../crates/ruvent/src/envelope/mod.rs))

```text
 고정 머리 28   id 16 · channel 4 · type 4 · flags 2 · failures 2
 작업 키        generation 4 · arrival varint                       (착수용, 서버가 보내거나 기록할 때)
 선택 필드      reply 4 · correlation 16 · principal 4 · delegate 4 · not_before varint · expires varint · group 16
 payload        길이 4 + 바이트
```

* 착수 블록은 엔벨롭에서 뺐다. 표지와 lease는 `Assign`·`Claimed` 레코드의 머리에 실린다.
* `not_before`와 `expires`는 등록 요청에서는 적용 순간부터의 길이이고, 서버가 쥐거나 보낼 때는 누적 가동
  시각의 절대값이다.
* 레코드 종류별로 실리는 필드는 `Layout`이 정한다(Request, Notice, Assigned, Delivery, Journal).
* `failures`는 이벤트 탓인 실패 횟수다(lease 만료 회수, `Release{실패 = 예}`). 65,535에서 멈춘다.

## 이름표

세 공간으로 나뉜다.

* 서버 안: 공유 핸들 `Name`, 사전 `Names`.
* 접속마다: 프로토콜의 방향별 사전. 상한을 넘으면 초기화 레코드를 쓴다.
* 파일마다: 저널 파일·스냅샷 파일이 각자 이름을 정의하므로 파일 하나만으로 해독된다.

## 저널 형식 3 ([crates/ruvent/src/journal](../crates/ruvent/src/journal/mod.rs))

레코드 집합(`Record`)과 세그먼트 컨테이너(`SegmentLog`)로 나뉜다. 바이트 배치와 태그 표는
`journal/codec.rs`(세그먼트)와 `journal/checkpoint/mod.rs`(체크포인트)의 doc comment에 있다.

```text
<dir>/lock               로그를 연 엔진 하나만
<dir>/0000000001.seg     세그먼트. 열(lane)은 셋이고 번호는 하나로 이어진다
<dir>/checkpoint.ckpt    마지막 완전한 체크포인트(rename으로 바뀐다)
세그먼트 머리: "rvseg\0" 0x03 lane · 번호 u32 · 첫 일련번호 u64
로그 프레임: [len u32][crc u32][kind][uptime u64][commit v][first v][next v][count v][item…]
```

| 열 | 담는 것 | 재생 |
| --- | --- | --- |
| 로그 | 레코드 프레임. 본문은 `Register` 안에 | 체크포인트 시작부터 |
| 오래 남는 열 | 이주한 본문(`[len][crc][본문]`) | 안 함 |
| 본문 열 | 작업 연결이 디스크로 흘려 읽은 후속 본문 | 안 함 |

* **본문은 세그먼트가 유일한 사본이다.** 본문 위치(`Location`: 세그먼트, 오프셋, 길이, CRC)는 등록이 디스크에
  닿을 때 장부에 들어간다. 본문 CRC는 한 번만 계산해 프레임 CRC에 합친다(`crc32fast::Hasher::combine`).
* **세그먼트 사전.** 세그먼트마다 쓰는 이름과 생산자 번호를 첫 사용 전에 스스로 정의한다.
* **넘김과 fsync.** 다음 프레임이 크기 S(`StorageConfig::segment_bytes`)를 넘기면 새 세그먼트로 넘긴다. 새
  세그먼트와 체크포인트 rename 뒤에는 디렉터리를 fsync한다(Windows는 NTFS 메타 저널에 맡긴다).
* **삭제.** 살아 있는 본문 0(그 본문을 옮긴 레코드가 디스크에 닿은 뒤에 센다), 읽기 pin 없음, 봉인됨, 로그
  열은 마지막 체크포인트 시작보다 통째로 앞일 때. 장부가 후보를 내고 엔진이 pin을 보고 잡일 스레드가 지운다.
* **이주.** 봉인된 세그먼트의 붙잡힌 죽은 바이트가 디스크 예산의 비율(S3)을 넘으면 살아 있는 비율이 가장
  낮은 세그먼트의 본문을 오래 남는 열로 복사하고 장부가 `Relocate`를 쓴다.
* **퍼지 체크포인트.** 꼬리가 max(세그먼트 1개, 이전 체크포인트 × 8)(상한 max(8 GiB, 이전))를 넘으면 장부가
  연산 묶음 사이에 슬롯을 조각씩 훑어 작성기 스레드로 넘긴다. 시작은 그 순간 디스크에 없던 첫 레코드의
  일련번호다. 재생은 항목의 일련번호보다 뒤의 레코드만 적용한다. 정상 종료 때는 멈춘 장부에서 통째로 쓴다.
* **복구는 스트리밍이다.** 체크포인트를 읽고 시작 세그먼트부터 꼬리를 프레임 단위로 읽는다. 본문은 CRC만
  계산하고 건너뛰므로 복구 메모리는 메타 크기다. 마지막 세그먼트의 찢어진 꼬리만 열 때 잘라 내고, 중간
  손상과 일련번호 사슬 끊김은 멈추고 보고한다.
* **옛 형식은 옮기지 않는다.** 경로가 파일이면(형식 2 이하, 형식 3의 단일 파일) 거부하고 그대로 둔다.
* **읽기 경로.** 캐시에 없는 본문은 blocking 읽기가 가져와 엔진 inbox로 돌려준다. 그동안 그 연결의 뒤 송신은
  기다리고 세그먼트는 pin된다. 콜드 머리를 보낸 줄은 다음 본문을 미리 읽는다(`read_ahead`).
* **흘려 읽기(B15 포함).** 작업 연결의 후속 본문은 `Engine::body_to_disk`가 예라고 하면 `Spool`로 본문 열에
  받고 `Call::Complete::spooled`로 넘긴다. 영속이면 `Register`가 그 위치를 가리키고, 휘발이면 저널에 없이
  재기동 때 버린다(`Counters::spilled_bytes`).

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
* 프록시([crates/ruvent-proxy](../crates/ruvent-proxy/README.md)): 발송 팬아웃 계층(개편 계획 4장)의 골조다. 서버 목록의
  모든 서버에 SDK 관찰자로 붙고(keepalive와 재접속은 SDK), 대상(채널 또는 접두사)마다 서버마다 상향 구독 하나를 두고
  구성원에게 나눠 보낸다. 구성원이 붙을 때마다 관찰자에 구독을 한 번 더 내고, 관찰자가 붙인 ticket 이상인 그 서버의
  확인(`Observed::Subscribed`) 이후의 발송만 그 구성원에게 보낸다. 엔벨롭은 허브가 `Arc`로 나눈다. 허브는 구독
  장부(`fanout/channels`, 순수 상태), 허브 태스크(`fanout/task.rs`), 핸들(`fanout/mod.rs`의 `Fanout`·`Member`)로
  나뉜다. 클라이언트 쪽 프로토콜, 인증, 등록(SDK 생산자), 요청–응답(SDK 요청자), 기한, 역압, 준비 상태의 노출은
  프로젝트의 것이고 이 크레이트에는 없다(`the_proxy_is_a_skeleton`이 막는다). 이벤트 서버는 TCP 위의 wire v2 하나만
  말하고, 프록시와 워커 서버만 그것으로 붙는다.
