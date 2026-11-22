# 사용법

## 1. 서비스를 쓴다

핸들러는 이벤트 하나를 받아 한 가지 일을 하고, 사실을 기록하고 끝난다.

```rust
use ruvent::{Audience, Bytes, Context, Envelope, Failure};
use std::sync::Arc;

async fn open_turn(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    let request: TurnRequest = command.json().map_err(Failure::fatal)?;
    // key: 이 사실이 "무엇인지". 재실행해도 같은 이벤트가 된다.
    cx.fact(format!("turn.started:{}", command.transaction_key), "turn.started", ruvent::json(&request), Audience::Client).await?;
    Ok(Bytes::new())   // 반환값은 `<action>.result` 이벤트의 payload
}
```

| 하고 싶은 일 | 호출 | 성질 |
| --- | --- | --- |
| 스트리밍 조각 내보내기 | `cx.progress(action, payload)` | 기다리지 않는다. 디스크에 안 쓴다. 낸 순서대로 로그에 들어간다 |
| 사실 기록 | `cx.fact(key, action, payload, audience).await` | 재실행해도 한 번. 등록된 반응자마다 일감이 발급된다 |
| 부수효과 직전 소유 확인 | `cx.fence().await` | 거짓이면 하지 않는다 |
| 영구 실패 | `Err(Failure::fatal(..))` | `.failure` 이벤트로 기록, 재시도 없음 |
| 나중에 다시 | `Err(Failure::retry(after))` | `after` 뒤 재실행 |

`Audience::Worker`로 낸 사실은 클라이언트에게 보이지 않고 반응자만 받는다.

## 2. 등록한다

```rust
use ruvent::{App, Service};

App::open("data/app.journal")
    .service(Service::new("turn")
        .accept("turn.run", open_turn)        // 클라이언트가 올릴 수 있는 명령
        .on("model.replied", decide))         // 기록된 사실에 대한 반응
    .service(Service::new("inference")
        .on("turn.started", call_model)
        .max_in_flight(64)                    // 하류가 감당하는 만큼
        .timeout(Duration::from_secs(300)))   // 스텝 하나의 상한
    .run()
    .await
```

* `Service` 하나가 큐 하나와 동시 실행 예산 하나다. **주제가 아니라 기다리는 방식으로** 묶는다:
  밀리초짜리 장부 정리, 수십 초짜리 추론, 검증 안 된 명령 실행은 서로 다른 서비스다.
* `accept`는 그 action을 허용 목록에 올리고 이 서비스로 라우팅한다. 한 명령의 주인은 하나다.
* `on`은 그 사실의 반응자에 이 서비스를 더한다. 반응자가 여럿이면 각자 자기 이벤트를 받는다.

### 모듈로 묶기

```rust
struct Billing { rates: Rates }

impl ruvent::Module for Billing {
    fn install(self, app: App) -> App {
        app.service(Service::new("billing").on("order.placed", move |cx, fact| charge(cx, fact)))
    }
}

App::open("data/app.journal").mount(Billing { rates }).mount(shipping_module)
```

## 3. 무엇을 디스크에 남길지 정한다

기본값은 "재시작이 이어가는 데 필요한 것만"이다([architecture.md](architecture.md) 5절).

```rust
use ruvent::{Durability, Events};

App::open("data/app.journal")
    // 이 채널의 사실은 재시작 뒤에도 읽을 수 있어야 한다
    .persist_events(Events::Channels(vec!["audit.".into()]))
    // 이 서비스의 일감은 적지 않는다. 크래시가 나면 클라이언트가 다시 요청한다
    .service(Service::new("preview").accept("preview.run", preview).transient())
    // 창을 넓히면 더 많은 일이 "왔다 간 일"이 되어 덜 쓴다. 잃는 폭도 그만큼 커진다
    .durability(Durability::Scheduled { window: Duration::from_millis(250) })
```

세부 조정은 `App::engine(EngineConfig { .. })`로 한다: `Retention`(메모리 상한),
`Persist`(디스크 대상), `Compaction`(스냅샷 임계), `max_attempts`.

## 4. 프로세스 안에서 쓰기

`App::start()`는 `Started`를 돌려준다.

```rust
let app = App::open("data/app.journal").service(..).start().await?;

// 정책을 거쳐 명령 제출 (전송 플러그인이 하는 일과 같다)
app.gateway().submit(&identity, proposal).await?;

// 사실을 직접 기록 (워커 실행이 아닌 곳에서)
app.engine().append(vec![Envelope::draft(Kind::Fact, "clock.ticked", "system", "{}")]).await?;

// 읽기
let mut room = app.engine().subscribe(vec!["room.42".into()], From::Now);
while let events = room.next(64).await { .. }

app.shutdown().await?;   // 수신 중단 → 진행 중인 스텝 완료 → 저널 기록 → 저널 해제
```

## 5. 전송 플러그인을 쓴다

게이트웨이는 프로토콜을 모른다. 전송은 자기 와이어를 게이트웨이 호출로 바꾸는 플러그인이다.

```rust
#[async_trait]
impl Transport for MyTransport {
    async fn run(self: Box<Self>, gateway: Gateway, shutdown: CancellationToken) -> io::Result<()> {
        // 연결이 있는 프로토콜:  gateway.serve(connection, identity).await
        // 연결이 없는 프로토콜:  gateway.submit(..) / gateway.poll(..)
    }
}
```

| 구현할 것 | 내용 |
| --- | --- |
| `Connection::recv` | 다음 `ClientFrame`. **취소 안전**해야 한다 |
| `Connection::send` | `ServerFrame` 하나를 와이어로 |
| 인증 | 핸드셰이크에서 `Credentials`를 뽑아 `gateway.authenticate` |

JSON 프레임이 필요하면 `ruvent::wire::json`을 쓴다. payload가 JSON이면 파싱 없이 그대로 싣는다.

## 6. 운영에서 볼 것

`Engine::stats()`:

| 값 | 뜻 |
| --- | --- |
| `journal_lag` | 지금 크래시가 나면 잃을 변화의 상한 |
| `retained_events`, `retained_bytes` | 메모리에 든 로그 |
| `journal_writes`, `compactions` | 디스크 활동 |
| `progress_dropped` | 엔진 수신함이 가득 차 버린 진행 이벤트 |
| `wakeups` | 엔진이 깨어난 횟수. 유휴 중에는 변하지 않아야 한다 |

`Engine::dead_letters(queue)`: 시도 횟수를 소진한 명령.
