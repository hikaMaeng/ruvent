# 사용법

## 내장 엔진 + 내장 워커

```rust
use ruvent::{connect, Engine, EngineConfig, SegmentLog};
use ruvent_client::worker::{App, Service};

# async fn demo() -> std::io::Result<()> {
let (journal, recovered) = SegmentLog::open("data/journal")?;
let engine = Engine::start(EngineConfig::default(), journal, recovered).await?;
let (client, inbox) = connect(&engine, "workers", 4096).await.expect("engine is up");
let running = App::new(client, inbox)
    .service(Service::new("orders").on("create", |cx| async move {
        cx.emit_dispatch("orders.events", "created", cx.payload().clone());
        Ok(())
    }))
    .start()
    .await
    .expect("subscribed");
tokio::select! {
    stopped = engine.stopped() => { stopped.expect("engine failed"); }
    _ = running.wait() => {}
}
# Ok(()) }
```

## 이벤트 서버

```sh
cargo run --release -p ruvent-server -- crates/ruvent-server/server.example.json
```

설정은 JSON 하나다. `listen`과 `credentials`만 필수이고, 나머지는 계획 8장의 값이 기본값으로 들어 있다.
고칠 값만 적는다.

```json
{
  "listen": "127.0.0.1:7400",
  "journal": "data/journal",
  "durability": "strict",
  "credentials": [
    { "name": "workers", "token": "…", "methods": ["register_claimable", "register_dispatch", "claim", "complete"], "channels": ["orders.*", "reply.*"] },
    { "name": "ops", "token": "…", "methods": ["operate"] }
  ],
  "memory": { "limit": 8589934592 },
  "storage": { "disk_capacity": 107374182400 },
  "channels": { "rules": [ { "pattern": "jobs.*", "isolate_after": 3 } ] },
  "connection": { "unsent": 268435456, "close_ms": 5000, "keepalive": { "idle_ms": 15000 } }
}
```

| 절 | 담는 것 |
| --- | --- |
| `journal` | 세그먼트 로그 디렉터리. 없으면 휘발 서버 |
| `credentials` | 자격마다 토큰, 메서드(`operate`는 `Stats`·`Drain`), 채널 패턴, 위임 허용 |
| `engine`, `producers` | 누적 확인 N(S2), 재접속 유예(S20), 후속 상한(S10), 정지 뒤 허용량(S14), 생산자 순번표(B4, S15) |
| `memory`, `storage` | 관리 메모리 상한과 수위(S5, S19, S22), 세그먼트·체크포인트·디스크 예산(S3, S4, S6) |
| `channels` | 채널 패턴별 격리 문턱(B6), 만료 격리(B9), `max_backlog`(B17), `serialize_group`(B26), 휘발 |
| `connection` | 사전 상한(S18), 본문 상한(B13), 작은 본문 문턱(S11), 관찰 연결 미송신 상한(S7), 닫기 기한(S20), keepalive(S13) |

모든 키와 기본값은 [crates/ruvent-server/README.md](../crates/ruvent-server/README.md)에 있다.
Ctrl-C로 정상 종료한다: drain(생산 연결에 최종 확인과 `Bye{draining}`) → 나머지 연결에 `Bye{draining}` →
엔진 종료와 체크포인트. 엔진이 스스로 멈추면 실패 코드로 끝난다.

## 원격 연결

```rust,ignore
let (client, inbox) = ruvent_client::remote::connect("127.0.0.1:7400", "workers-secret-change-me", ruvent::Hello::worker("ignored"), Default::default()).await?;
```

`Hello`가 역할(생산·작업·관찰)과 생산자 id, 인스턴스 nonce를 정한다. 돌려받는 `Client`는 인프로세스 연결과 같다.
재접속·미확인 버퍼·서버 선택·워커 규칙은 SDK 계층(계획 2.9)의 몫이다.

## 프록시

프록시는 배포용 바이너리가 없다. `ruvent-proxy`에 의존하는 프로그램을 만든다. 프로토콜은 feature(`ws`, `http`,
기본은 둘 다)로 고르고, 켠 프로토콜은 한 `listen` 주소에서 함께 열린다.

```rust,ignore
#[tokio::main]
async fn main() -> Result<(), ruvent_proxy::LaunchError> {
    ruvent_proxy::Server::new().run().await              // 설정대로
    // ruvent_proxy::Server::new().hooks(Domain).run().await   // 도메인 훅을 붙여서
}
```

```sh
cargo run --release -p ruvent-proxy --example plain -- crates/ruvent-proxy/proxy.example.json
cargo run --release -p ruvent-proxy --example custom -- crates/ruvent-proxy/proxy.example.json
```

* WS: `GET /v1/ws?token=…` 뒤에 JSON 메시지(`publish`, `subscribe`, `unsubscribe`)를 주고받는다.
* HTTP: `POST /v1/events`(Bearer 토큰)로 등록, `GET /v1/stream?channel=…`(SSE)로 받는다.
* 훅: `authenticate`, `subscribe`, `publish`, `deliver` 중 필요한 것만 쓴다. 쓰지 않은 훅은 설정의 인증 모드
  (`none`, `static`, `worker`)와 채널 범위를 따른다. 훅, 메시지 형식, 설정은
  [crates/ruvent-proxy/README.md](../crates/ruvent-proxy/README.md).
* 멈추기: `run()`은 Ctrl-C에, `run_until(신호)`는 그 신호에 끝난다. 끝나면 사용자 연결이 모두 닫히고 상위 연결도
  놓는다. 바인드만 하고 직접 서비스하려면 `bind()` 뒤 `Bound::serve_until(신호)`.

## 워커가 정하는 것

* 재시도 상한: `Service::max_retries`. 엔벨롭 `retry`(서버가 회수 때 올린 값)가 넘으면 핸들러를 돌리지 않고
  완료한다.
* 실패 보고: `Service::failures(채널)`을 설정했을 때만 실패 발송 이벤트를 낸다.
* 긴 일: 핸들러가 작업용 착수 이벤트를 후속으로 내고 자기 이벤트를 완료한다. 그 일은
  `Service::executor(채널)`(별도 예산, 긴 lease)이 처리한다.
* 외부 효과 중복: `Context::key()`(작업 키)로 거른다. 회수와 재기동에도 바뀌지 않는다.
