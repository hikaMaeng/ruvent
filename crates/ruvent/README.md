# ruvent (코어)

이벤트 서버의 장부와 엔진이다. 소켓은 없다. 인프로세스에서 바로 쓰거나 [`ruvent-server`](../ruvent-server/README.md)
뒤에 둔다. 설계는 [개편 계획 v4.6](../../docs/proposals/library-overhaul.md)을 따른다.

| 모듈 | 내용 |
| --- | --- |
| `names` | 공유 이름 핸들 `Name`, 서버 사전 `Names`(쓸어내기로 크기 상한), 채널 패턴 `Pattern` |
| `envelope` | 단일 엔벨롭 `Envelope`(u16 flags, `not_before`·`expires`·`group`, 실패 횟수)과 `EventId`·`WorkKey`(u64 도착 순번)·`ClaimToken`(u64)·`Digest`. `envelope::codec`이 바이트 배치, 가변 길이 정수, 레코드 종류별 규칙(`Layout`)을 정한다 |
| `journal` | 형식 3 레코드 집합 `Record`, 쓰기 묶음 `Batch`(누적 가동 시각·커밋 번호·레코드 일련번호), `Journal` 계약, 단일 파일 컨테이너 `FileJournal`(형식 2 이하는 거부), `Volatile` |
| `snapshot` | 운영 스냅샷 수집기와 파일 형식(`snapshot::read`) |
| `engine` | 단일 작성자 엔진 태스크(`Engine`), 장부(`engine::state`), 설정(`EngineConfig`과 8장 값), 연산 값(`Call`, `Outbound`, `Hello`, `Assignment`, `Stats`, 코드 표 `ByeReason`·`FaultKind`·`Refusal`) |
| `methods` | 연산 계약 `Methods`, 그 유일한 구현 `Client`, 응답 분배 `Demux`, 인프로세스 연결 `connect` |

## 연결 역할

연결은 열 때 역할이 정해진다(`Hello::producer`, `Hello::worker`, `Hello::observer`).

| 역할 | 보내는 것 | 받는 것 |
| --- | --- | --- |
| 생산 | `Register{seq}`, `Delete{seq}` | 누적 `Committed`, `Paused`/`Resumed`, `Fault` |
| 작업 | `Join`, `Window`, `Complete{seq}`, `Extend`, `Release`, `Claim{id}`, 발송용 `Register` | `Assign`, `Pending`, `Committed`, 참여한 채널의 발송(발송 포함일 때) |
| 관찰 | `Subscribe`, `Unsubscribe`, `Stats`, `Drain` | `Notice`, `Delivery`, `StatsReply` |

## 최소 사용

```rust
use ruvent::{connect, Bytes, Engine, EngineConfig, Envelope, Hello, Incoming, Methods};
use std::time::Duration;

# async fn demo() {
let engine = Engine::volatile(EngineConfig::default()).await;
let (producer, _) = connect(&engine, Hello::producer("svc"), 1024).await.unwrap();
let (worker, mut inbox) = connect(&engine, Hello::worker("svc"), 1024).await.unwrap();
worker.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
worker.window("work".into(), 16, 0).await.unwrap();
producer.register_committed(Envelope::claimable("work", "do", Bytes::from_static(b"{}"))).await.unwrap();
if let Some(Incoming::Assign(assignment)) = inbox.recv().await {
    let seq = worker.complete(assignment.event.id, assignment.token, Vec::new()).await.unwrap();
    worker.committed(seq).await.unwrap();
}
# }
```

파일 저널: `let (journal, recovered) = FileJournal::open(path)?; Engine::start(config, journal, recovered).await?`.
새 세대가 디스크에 기록된 뒤에 `start`가 돌아온다. 정상 종료(`shutdown`)는 멈춘 장부의 snapshot을 마지막으로 쓴다.

## 설정 (`EngineConfig`, 계획 8장)

| 필드 | 기본값 | 뜻 |
| --- | --- | --- |
| `durability` | `Scheduled` | `Strict`면 영속 채널 이벤트가 커밋 전에는 배정·통지·backlog·명시 착수 어디에도 보이지 않는다. 두 모드 모두 `Committed`는 fsync 뒤 |
| `ack_every` | 256 | S2: 기다리는 것이 남아도 확인할 수 있는 미확인이 이만큼이면 `Committed`를 보낸다 |
| `producers` | 1시간 · 7일 · 100만 · 64 B | B4·S15: 생산자 순번표 보존(`linger`), 요청 상한, 항목 상한, 항목당 회계 바이트 |
| `reconnect_grace` | 3초 | S20: 닫힌 연결의 다 쓴 배정의 기한 |
| `max_follow_ups` | 1,024 | S10 |
| `memory` | 물리 메모리 50% · 예약 10% · 고수위 80% · 해제 7/8 · 조기 정지 1/8 · 순증 256 · 메타 180 B · 깊이 목표 1억 | S5·S19·S22 |
| `pause_allowance` | 1 MiB | S14 |
| `stats_top` | 16 | B24 |
| `channels` | `isolate_after` 5, 격리 보존 7일, `serialize_group` 꺼짐, `reply.*` 휘발·격리 없음, `isolated.*` 격리 없음 | B6·B9·B17·B26·휘발 채널. 패턴별로 덮는다 |
| `storage` | 세그먼트 64 MiB 등 | S3·S4·S6·S17. 세그먼트 컨테이너가 읽는다 |
| `batch_max`, `inbox`, `outbound`, `backlog_page` | 2048, 65,536, 65,536, 1024 | 엔진 묶음, 연산함, 연결 송신 버퍼(개수), backlog 한 쪽 |
| `compaction` | 50,000 · 4배 | 단일 파일 컨테이너의 snapshot 다시 쓰기 계기 |
| `grants` | `None`(모두 허용) | 서비스 자격별 메서드(`Operate` 포함)·채널·위임 권한 |
| `snapshot` | `None` | 운영 스냅샷 |

시험 지도는 [docs/testing.md](../../docs/testing.md).
