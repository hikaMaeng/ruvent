# 사용법

## 내장 엔진 + 내장 워커

SDK는 연결 방법(`link::Connector`)만 바꿔 끼운다. 프로세스 안 엔진은 `link::Local`에 이름으로 넣는다.

```rust,ignore
use ruvent::{Engine, EngineConfig, SegmentLog};
use ruvent_client::link::Local;
use ruvent_client::servers::Servers;
use ruvent_client::worker::{Retry, Service, Worker};
use std::sync::Arc;
use std::time::Duration;

let (journal, recovered) = SegmentLog::open("data/journal")?;
let engine = Engine::start(EngineConfig::default(), journal, recovered).await?;
let local = Arc::new(Local::new());
local.insert("main", engine.clone());
let servers = Servers::new(["main"]);
let worker = Worker::builder(local, &servers)
    .service(Service::new("orders").slots(16).on("create", |cx| async move {
        if downstream_is_down() {
            return Err(Retry::later(Duration::from_secs(5)));   // 이벤트 탓 아님: 실패 횟수 그대로
        }
        cx.emit_dispatch("orders.events", "created", cx.payload().clone());
        Ok(())                                                  // Complete(후속 포함)
    }))
    .start();
// …
worker.shutdown().await;   // 창 0 → 유예 → 남은 실행은 실패 아님으로 반환
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

## 원격 연결 (SDK)

서버 목록과 토큰 하나로 붙는다. 서버마다 역할별 연결을 열고, 끊기면 지수 백오프 + full jitter(50 ms ~ 1초,
`draining` 뒤 30초)로 다시 붙는다. 모든 소켓에 keepalive를 건다.

```rust,ignore
use ruvent::Envelope;
use ruvent_client::link::Tcp;
use ruvent_client::producer::{Producer, ProducerConfig};
use ruvent_client::requester::{RequestOptions, Requester, RequesterConfig};
use ruvent_client::servers::Servers;
use std::sync::Arc;

let tcp = Arc::new(Tcp::new("workers-secret-change-me"));
let servers = Servers::new(["10.0.0.1:7400", "10.0.0.2:7400"]);   // `servers.set(…)`로 바꾸면 붙고 뗀다

// 생산자: 선택기가 서버를 고르고, 누적 확인이 올 때까지 미확인 버퍼에 쥔다(가득이면 기다린다).
let producer = Producer::start(tcp.clone(), &servers, ProducerConfig::default());
producer.send(Envelope::claimable("orders", "create", payload)).await?;
producer.flush().await?;                      // 지금까지 보낸 것이 모두 커밋될 때까지

// 요청자: 인스턴스마다 응답 채널 `reply.<id>` 하나. 결과는 착수용 이벤트라 재접속·재기동에도 잃지 않는다.
let requester = Requester::start(tcp.clone(), &servers, RequesterConfig::default());
let response = requester.request(Envelope::claimable("orders", "price", body), RequestOptions::default()).await?;
// response.fragments(스트림 조각) → response.result 순서. response.fragments_lost: 그사이 연결이 바뀌었다
```

워커 핸들러에서 결과는 `cx.reply(payload)`(응답 채널의 후속, `expires` 60초), 조각은 `cx.fragment(payload).await`
(작업 연결로 바로 방출)로 낸다. 관찰자(`observer::Observer`)는 구독을 연결마다 다시 내고 발송 엔벨롭을 그대로
넘긴다. 끊긴 동안의 발송은 `Observed::Gap`으로 알린다. 서버의 구독 확인은 `Observed::Subscribed { server, target, ticket }`로
스트림에 들어온다. ticket은 관찰자가 처리한 `subscribe` 호출의 순번이고, k번째 호출은 그 서버에서 ticket ≥ k인 첫 확인부터
효력이 있다(그 뒤의 발송은 구독이 선 뒤에 보내진 것). 요청자의 응답 채널 참여 변화는 `Requester::watch_joined()`로 기다린다.

| `Bye` 사유 | SDK |
| --- | --- |
| `superseded` | 그 생산자 id로 다시 붙지 않고 오류(`ProducerError::Refused`) |
| `draining` | 생산 후보에서 빼고, 최종 확인 뒤의 미확인분은 다른 서버로. 긴 백오프로 다시 붙는다 |
| `slow` | 다시 붙는다. 그사이 발송은 잃는다 |
| `version`, `token`, `forbidden` | 다시 붙지 않고 오류 |

소켓 한 줄만 쓰려면 `ruvent_client::remote::connect`(전송층, 재접속 없음)를 쓴다.

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

* 상향: 설정의 `upstream.servers`에 적은 **모든 서버**에 붙는다. 서버마다 관찰(구독), 생산(등록), 작업(응답 채널)
  연결이 하나씩이고 모두 SDK의 것이다(재접속, keepalive, `Bye` 사유표). 라우터가 관리하는 목록이나 같은 프로세스
  엔진은 `Server::upstream(Upstream { connector, servers, credential })`로 넘긴다.
* WS: `GET /v1/ws?token=…` 뒤에 JSON 메시지(`publish`, `subscribe`, `unsubscribe`)를 주고받는다. 구독은 정확한
  `channel` 또는 `prefix`(그 아래 `.` 경계의 모든 채널) 하나를 적는다.
* HTTP: `POST /v1/events`(Bearer 토큰)로 등록하고 커밋되면 200이다. `GET /v1/stream?channel=…&prefix=…`(SSE)로 받는다.
* `/health`: 목록의 모든 서버에서 관찰 연결과 응답 채널 참여가 살아 있으면 200, 아니면 503. 본문에 서버별 상태가 있다.
* 요청–응답: `Proxy::exchange(event)`(또는 훅의 `cx.upstream()?.exchange(…)`). 결과는 프록시 인스턴스의 응답 채널
  `reply.<인스턴스 id>`에 착수용 이벤트로 오고, 상향이 끊겨도 기다리던 결과는 다시 붙으면 온다. 조각은 결과보다
  앞서 같은 연결로 온다(`exchange_stream`).
* 원 엔벨롭: 자기 프로토콜을 붙이는 프로그램은 `Proxy::attach_raw`로 `Outgoing::Event(Arc<Envelope>)`를 받는다.
  payload를 JSON 텍스트로 바꾸지 않는다.
* 훅: `authenticate`, `subscribe`(대상이 채널 또는 접두사), `publish`, `deliver` 중 필요한 것만 쓴다. 쓰지 않은 훅은
  설정의 인증 모드(`none`, `static`, `worker`)와 채널 범위를 따른다. `worker` 인증은 요청–응답 하나이고, 워커는
  요청의 `reply` 채널에 착수용 후속으로 답한다. 훅, 메시지 형식, 설정, 오류 → 상태 코드는
  [crates/ruvent-proxy/README.md](../crates/ruvent-proxy/README.md).
* 멈추기: `run()`은 Ctrl-C에, `run_until(신호)`는 그 신호에 끝난다. 끝나면 사용자 연결이 모두 닫히고 상향 연결도
  모두 놓는다. 바인드만 하고 직접 서비스하려면 `bind()` 뒤 `Bound::serve_until(신호)`.

## 워커가 정하는 것

* 핸들러의 끝은 셋이다: `Ok(())`(완료, 영구 실패도 실패 후속과 함께 완료), `Err(Retry::later(지연))`(기본, 이벤트
  탓 아님: 실패 횟수 그대로), `Err(Retry::failed(지연))`(이벤트 탓: 실패 횟수가 오른다). 몇 번까지인지는 서버의
  `isolate_after`(기본 5)가 정하고, SDK는 횟수를 세지 않는다. 핸들러 없이 완료하는 경로도 없다.
* lease(기본 30초)는 SDK가 절반마다 자동으로 연장한다. 작업의 최대 시간은 실행 한도(이벤트 워커 30초,
  `Service::executor` 1시간)다. 넘으면 끊고 `Release{실패 = 예}`로 돌려준다. 정지된 채널로의 `cx.send`를
  기다리다 넘긴 실행은 실패로 세지 않는다.
* 슬롯 P(`Service::slots`)를 서버들의 창에 나눈다. 창의 합은 P를 넘지 않고, 굶는 서버(`Pending`)가 있으면 슬롯
  하나씩 옮긴다(계획 2.4).
* 비가역 부작용은 업무 키의 유일 제약이나 하류 멱등 키로, 덮어쓰기는 `cx.token()`(펜싱 토큰)으로 막는다.
  `Context::key()`(작업 키)는 회수와 재기동에도 바뀌지 않는다.
