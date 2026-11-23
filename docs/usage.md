# 사용법

## 내장 엔진 + 내장 워커

```rust
use ruvent::{connect, Engine, EngineConfig, FileJournal};
use ruvent_client::worker::{App, Service};

# async fn demo() -> std::io::Result<()> {
let (journal, recovered) = FileJournal::open("data/events.journal")?;
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

설정 필드는 [crates/ruvent-server/README.md](../crates/ruvent-server/README.md)에 있다. 자격마다 토큰,
메서드, 채널 패턴, 위임 허용을 둔다. Ctrl-C로 정상 종료하고, 엔진이 멈추면 실패 코드로 끝난다.

## 원격 워커 서버

```rust
# async fn demo() -> std::io::Result<()> {
let (client, inbox) = ruvent_client::remote::connect("127.0.0.1:7400", "workers-secret-change-me", Default::default()).await?;
let running = ruvent_client::worker::App::new(client, inbox) /* .service(...) */ .start().await.expect("subscribed");
running.wait().await.ok();
# Ok(()) }
```

같은 `Service`를 인프로세스와 원격에 그대로 붙인다.

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

## 워커가 정하는 것

* 재시도 상한: `Service::max_retries`. 엔벨롭 `retry`(서버가 회수 때 올린 값)가 넘으면 핸들러를 돌리지 않고
  완료한다.
* 실패 보고: `Service::failures(채널)`을 설정했을 때만 실패 발송 이벤트를 낸다.
* 긴 일: 핸들러가 작업용 착수 이벤트를 후속으로 내고 자기 이벤트를 완료한다. 그 일은
  `Service::executor(채널)`(별도 예산, 긴 lease)이 처리한다.
* 외부 효과 중복: `Context::key()`(작업 키)로 거른다. 회수와 재기동에도 바뀌지 않는다.
