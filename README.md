# ruvent

이벤트를 발행하고 수신하는 방식으로 서비스를 작성하는 Rust 프레임워크다.
요청과 응답이 아니라 **이벤트와 그에 대한 반응**이 서비스의 단위다.

독립 서버가 아니라 라이브러리다. 함수를 등록하면 그 프로세스가 서버가 된다.

```rust
use ruvent::{App, Audience, Bytes, Context, Envelope, Failure, Service};
use std::sync::Arc;

async fn open_turn(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.fact(format!("turn.started:{}", command.transaction_key), "turn.started", "{}", Audience::Client).await?;
    Ok(Bytes::new())
}

async fn call_model(cx: Context, _fact: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.progress("model.delta", "\"안녕\"");          // 기다리지 않는 스트리밍
    cx.fact("model.replied:1", "model.replied", "{}", Audience::Worker).await?;
    Ok(Bytes::new())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    App::open("data/app.journal")
        .service(Service::new("turn").accept("turn.run", open_turn))
        .service(Service::new("inference").on("turn.started", call_model).max_in_flight(64))
        .run()
        .await
}
```

큐 이름도 리액션 표도 쓰지 않았다. `accept`가 클라이언트 허용 목록과 라우트를,
`on`이 리액션 표를 만든다. 어떤 함수도 다음 함수를 지목하지 않는다.

## 문서

| 문서 | 내용 |
| --- | --- |
| [docs/overview.md](docs/overview.md) | 무엇이고 무엇이 아닌가, 핵심 개념 |
| [docs/architecture.md](docs/architecture.md) | 단일 작성자 엔진, 이벤트 구동, 순변화 저널 |
| [docs/usage.md](docs/usage.md) | 서비스 작성, 핸들러 규칙, 전송 플러그인 |
| [docs/constraints.md](docs/constraints.md) | 보장하는 것과 보장하지 않는 것 |
| [docs/origin.md](docs/origin.md) | 원본(ndx_business `packages/agent`) 분석과 대응표 |
| [docs/testing.md](docs/testing.md) | 테스트가 증명하는 것, 실측 수치 |

## 구성

| 경로 | 책임 |
| --- | --- |
| `crates/ruvent/src/app/` | 프레임워크 표면: `App`, `Service`, `Module` |
| `crates/ruvent/src/engine/` | 단일 작성자 엔진: 큐, 실행, 로그, 보존·영속 정책, 저널 스케줄러 |
| `crates/ruvent/src/journal/` | 내구성 플러그인과 파일 저널 |
| `crates/ruvent/src/worker/` | 워커 틀: 소유, 스텝 기한, 펜싱, 종결 기록 |
| `crates/ruvent/src/gateway/` | 전송 중립 클라이언트 경계: 인증, 정책, 구독 |
| `crates/ruvent/src/wire/` | 전송 플러그인이 공유하는 JSON 프레임 |
| `crates/ruvent/src/envelope/` | 고정 엔벨롭 계약 |

## 확인

```sh
cargo test -p ruvent
cargo run --release --example bench
```
