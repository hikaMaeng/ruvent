# ruvent (코어)

이벤트 서버의 장부와 엔진이다. 소켓은 없다. 인프로세스에서 바로 쓰거나 [`ruvent-server`](../ruvent-server/README.md)
뒤에 둔다. 설계는 [개편 계획 v4.9](../../docs/proposals/library-overhaul.md)를 따른다(지금의 계약은 9.14).

| 모듈 | 내용 |
| --- | --- |
| `names` | 공유 이름 핸들 `Name`, 서버 사전 `Names`(쓸어내기로 크기 상한). 남은 이름은 자격과 대리 사용자뿐이다 |
| `types` | 타입 집합 `TypeSet`(정렬된 구간 목록, 이진 탐색). 설정·권한·스냅샷이 이것으로 타입을 고른다. 글 형식은 `*`, `N`, `A-B` |
| `envelope` | 단일 엔벨롭 `Envelope`(머리 38 B: id 16 · 채널 16 · 타입 u16 · flags u16 · 실패 u16), 채널 `Channel`(UUIDv7만 받는다), `EventId`·`WorkKey`·`ClaimToken`·`Digest`, 서버 타입 `SERVER_TYPE`(0). `envelope::codec`이 바이트 배치와 레코드 종류별 규칙(`Layout`)을 정한다 |
| `journal` | 형식 5 레코드 집합 `Record`, 쓰기 묶음 `Batch`, `Journal` 계약과 곁의 부품(`Checkpoints`, `Chores`, `Bodies`, `Spool`), 세그먼트 컨테이너 `SegmentLog`(형식 4 이하는 거부), 스트리밍 복구 `Recovered`/`Source`/`Replay`, `Volatile` |
| `snapshot` | 운영 스냅샷 수집기와 파일 형식(`snapshot::read`). 고르는 기준은 타입 집합 |
| `engine` | 단일 작성자 엔진 태스크(`Engine`), 장부(`engine::state`), 설정(`EngineConfig`, 타입별 설정 `TypeConfig`), 연산 값(`Call`, `Outbound`, `Hello`, `Assignment`, `Stats`, 코드 표 `ByeReason`·`FaultKind`·`Refusal`, 삭제 결과 `delete_outcome`) |
| `methods` | 연산 계약 `Methods`, 그 유일한 구현 `Client`, 응답 분배 `Demux`, 인프로세스 연결 `connect` |

## 채널과 타입

* **채널**은 이름 없는 UUIDv7(`Channel`, 16 B)이다. 보통 요청 하나가 채널 하나다. 서버는 채널에서 아무것도 읽지 않는다:
  계층도 접두사도 패턴도 없고, 버전 7·RFC 변형이 아니면 거절한다. 그래서 요청마다 새 채널이어도 사전 항목도 설정도 남지 않는다.
* **타입**은 u16이다. 서버가 착수용 이벤트를 워커에게 배정할 때 쓰고(`Join`), 설정·권한·스냅샷·정지가 타입을 키로 한다.
  0은 서버 것(삭제 결과)이라 생산자가 등록할 수 없다. 무엇을 뜻하는지는 프로젝트가 번호를 매긴다.
* **페이로드**는 임의 바이트다. 서버는 읽지 않는다.

## 연결 역할

연결은 열 때 역할이 정해진다(`Hello::producer`, `Hello::worker`, `Hello::observer`).

| 역할 | 보내는 것 | 받는 것 |
| --- | --- | --- |
| 생산 | `Register{seq}`, `Delete{seq}` | 누적 `Committed`, `Paused`/`Resumed`(뿌리 타입), `Fault` |
| 작업 | `Join`(채널, 타입 집합, 또는 둘. 격리된 것만 받는 참여도), `Window{join}`, `Complete{seq}`, `Extend`, `Release`, `Claim{id}`, 발송용 `Register` | `Assign`, `Pending{join}`, `Committed`, 참여한 채널의 발송(발송 포함일 때) |
| 관찰 | `Subscribe{channel}`, `Unsubscribe`, `Stats`, `Drain` | `Notice`(격리되면 격리 비트), `Delivery`, `StatsReply` |

## 최소 사용

```rust
use ruvent::{connect, Bytes, Channel, Engine, EngineConfig, Envelope, Hello, Incoming, Methods};
use std::time::Duration;

const CALL: u16 = 1;

# async fn demo() {
let engine = Engine::volatile(EngineConfig::default()).await;
let (producer, _) = connect(&engine, Hello::producer("svc"), 1024).await.unwrap();
let (worker, mut inbox) = connect(&engine, Hello::worker("svc"), 1024).await.unwrap();
// 타입 참여: 어느 채널이든 타입 CALL의 일을 받는다.
let join = worker.join(None, vec![CALL], false, Duration::from_secs(30), false).await.unwrap();
worker.window(join, 16, 0).await.unwrap();
let request = Channel::new(); // 요청 하나의 스트림
producer.register_committed(Envelope::claimable(request, CALL, Bytes::from_static(b"{}"))).await.unwrap();
if let Some(Incoming::Assign(assignment)) = inbox.recv().await {
    let seq = worker.complete(assignment.event.id, assignment.token, Vec::new()).await.unwrap();
    worker.committed(seq).await.unwrap();
}
# }
```

세그먼트 로그: `let (journal, recovered) = SegmentLog::open_with(dir, config.storage.segment_bytes)?; Engine::start(config, journal, recovered).await?`.
복구는 체크포인트와 꼬리를 디스크에서 흘려 읽고(본문은 읽지 않는다), 새 세대가 디스크에 기록된 뒤에 `start`가 돌아온다.
정상 종료(`shutdown`)는 멈춘 장부의 체크포인트를 마지막으로 쓴다. 저장의 구조는 [docs/architecture.md](../../docs/architecture.md)의 "저널 형식 5".

## 설정 (`EngineConfig`, 계획 8장)

| 필드 | 기본값 | 뜻 |
| --- | --- | --- |
| `durability` | `Scheduled` | `Strict`면 영속 이벤트가 커밋 전에는 배정·통지·backlog·명시 착수 어디에도 보이지 않는다. 두 모드 모두 `Committed`는 fsync 뒤 |
| `ack_every` | 256 | S2: 기다리는 것이 남아도 확인할 수 있는 미확인이 이만큼이면 `Committed`를 보낸다 |
| `producers` | 1시간 · 7일 · 100만 · 64 B | B4·S15: 생산자 순번표 보존(`linger`), 요청 상한, 항목 상한, 항목당 회계 바이트 |
| `reconnect_grace` | 3초 | S20: 닫힌 연결의 다 쓴 배정의 기한 |
| `max_follow_ups` | 1,024 | S10 |
| `memory` | 물리 메모리 50% · 예약 10% · 고수위 80% · 해제 7/8 · 조기 정지 1/8 · 순증 256 · 메타 180 B · 깊이 목표 1억 | S5·S19·S22. 정지의 주인은 뿌리 타입(계보를 시작한 이벤트의 타입) |
| `pause_allowance` | 1 MiB | S14 |
| `stats_top` | 16 | B24 |
| `types` | `isolate_after` 5, 격리 보존 7일, 만료 격리 꺼짐, 대기 상한 없음, `serialize_group` 꺼짐, 휘발 꺼짐 | B6·B9·B17·B26·휘발. 타입 집합 규칙(`TypeRule`)이 덮는다. 첫 규칙이 정한다 |
| `storage` | 세그먼트 64 MiB · 이주 10% · 조각 4,096 · 꼬리 8배 · 꼬리 상한 8 GiB · 디스크 예약 4세그먼트/10% · 용량 없음 · 미리 읽기 16 · 동시 읽기 64 · 조각 대기 2 | S3·S4·S6·S17. `disk_capacity`를 주어야 디스크 예산·고수위·이주 계기가 선다 |
| `batch_max`, `inbox`, `outbound`, `backlog_page` | 2048, 65,536, 65,536, 1024 | 엔진 묶음, 연산함, 연결 송신 버퍼(개수), backlog 한 쪽 |
| `grants` | `None`(모두 허용) | 서비스 자격별 메서드(`Operate` 포함)·타입 집합·위임 권한. 구독과 발송 참여는 자격의 타입만 받는다(타입 0은 늘) |
| `snapshot` | `None` | 운영 스냅샷(타입 집합) |

## 격리 (B6)

실패 횟수가 타입의 `isolate_after`에 닿으면 이벤트는 **제자리에서** 격리된다. 채널·타입·뿌리·내구성은 그대로이고 격리 비트가 켜진다.
기다리는 곳은 그 이벤트의 격리 줄이고, 가져가는 것은 `isolated: true`로 참여한 워커뿐이다. 채널 구독자는 격리 비트가 켜진 `Notice`를 받는다.
보존 기한(`isolated_keep`)이 지나면 지워진다. 다시 실패해도 횟수만 바뀌고 기한은 그대로다.

시험 지도는 [docs/testing.md](../../docs/testing.md).
