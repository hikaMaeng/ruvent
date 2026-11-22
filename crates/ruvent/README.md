# ruvent (코어)

이벤트 서버의 엔진이다. 소켓은 없다. 인프로세스에서 바로 쓰거나 [`ruvent-server`](../ruvent-server/README.md)
뒤에 둔다.

| 모듈 | 내용 |
| --- | --- |
| `names` | 공유 이름 핸들 `Name`, 서버 사전 `Names`(쓸어내기로 크기 상한), 채널 패턴 `Pattern` |
| `envelope` | 단일 엔벨롭 `Envelope`과 `EventId`(UUIDv7)·`WorkKey`·`ClaimToken`·`Claim`. `envelope::codec`이 바이트 배치와 레코드 종류별 규칙(`Layout`)을 정한다 |
| `journal` | `Journal` 계약, `FileJournal`(형식 v2: 배치 프레임, 파일마다 이름 정의), `Volatile` |
| `snapshot` | 운영 스냅샷 수집기와 파일 형식(`snapshot::read`) |
| `engine` | 단일 작성자 엔진 태스크(`Engine`), 상태 기계(`engine::state`), 설정(`EngineConfig`, `Grants`), 메서드 값(`Call`, `Reply`, `Outbound`) |
| `methods` | 메서드 계약 `Methods`, 그 유일한 구현 `Client`, 응답 분배 `Demux`, 인프로세스 연결 `connect` |

## 최소 사용

```rust
use ruvent::{connect, Bytes, Engine, EngineConfig, Envelope, Methods};
use std::time::Duration;

# async fn demo() {
let engine = Engine::volatile(EngineConfig::default()).await;
let (client, mut inbox) = connect(&engine, "svc", 1024).await.unwrap();
client.subscribe("work".into(), None).await.unwrap();
let (id, _key) = client.register_claimable(Envelope::claimable("work", "do", Bytes::from_static(b"{}"))).await.unwrap();
let claimed = client.claim(id, 1, Duration::from_secs(30)).await.unwrap();
client.complete(id, claimed.claim.unwrap().token, Vec::new()).await.unwrap();
# }
```

파일 저널: `let (journal, recovered) = FileJournal::open(path)?; Engine::start(config, journal, recovered).await?`.
새 세대가 디스크에 기록된 뒤에 `start`가 돌아온다.

## 설정 (`EngineConfig`)

| 필드 | 기본값 | 뜻 |
| --- | --- | --- |
| `durability` | `Scheduled` | `Strict`면 착수용 등록·완료(후속 포함)·삭제의 응답과 통지를 디스크 기록 뒤에 낸다. 착수·회수는 기다리지 않는다 |
| `batch_max` | 2048 | 한 배치에 적용하는 연산 수 상한 |
| `inbox` | 65,536 | 엔진 연산함 용량. 가득 차면 호출자가 기다린다 |
| `outbound` | 65,536 | 접속별 송신 버퍼. 가득 차면 그 접속을 끊는다 |
| `backlog_page` | 1024 | 구독 응답 한 번의 대기분 상한 |
| `compaction` | 50,000 · 4배 | 저널 레코드가 이만큼 넘고 생존분의 4배를 넘으면 스냅샷으로 다시 쓴다 |
| `grants` | `None`(모두 허용) | 서비스 자격별 메서드·채널·위임 권한 |
| `snapshot` | `None` | 운영 스냅샷: 채널·종류, 단위(개수·나이), 버퍼 |

자세한 설계는 [docs/architecture.md](../../docs/architecture.md).
