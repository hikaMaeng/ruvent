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
    { "name": "workers", "token": "…", "methods": ["register_claimable", "register_dispatch", "claim", "complete"], "channels": ["orders.*", "answer.*"] },
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
`draining` 뒤에도 같다)로 다시 붙는다. 모든 소켓에 keepalive를 건다.

```rust,ignore
use ruvent::Envelope;
use ruvent_client::link::Tcp;
use ruvent_client::producer::{Producer, ProducerConfig};
use ruvent_client::observer::{Observed, Observer, ObserverConfig};
use ruvent_client::servers::Servers;
use std::sync::Arc;

let tcp = Arc::new(Tcp::new("workers-secret-change-me"));
let servers = Servers::new(["10.0.0.1:7400", "10.0.0.2:7400"]);   // `servers.set(…)`로 바꾸면 붙고 뗀다

// 생산자: 선택기가 서버를 고르고, 누적 확인이 올 때까지 미확인 버퍼에 쥔다(가득이면 기다린다).
let producer = Producer::start(tcp.clone(), &servers, ProducerConfig::default());
producer.send(Envelope::claimable("orders", "create", payload)).await?;
producer.flush().await?;                      // 지금까지 보낸 것이 모두 커밋될 때까지

// 요청–응답: SDK 헬퍼가 없다. 요청마다 응답 채널 하나를 구독하고, 그 이름을 payload에 넣어 등록한다.
// 서버가 채널로 나누므로 받는 쪽은 분배하지 않는다. 기한·재전송·끝 조건은 묻는 쪽이 정한다.
let (observer, mut inbox) = Observer::start(tcp.clone(), &servers, ObserverConfig::default());
let answer = format!("answer.{}", ruvent::EventId::new_v7());
observer.subscribe(ruvent::Target::Channel(answer.as_str().into())).await?;
producer.send(Envelope::claimable("chat", "complete", my_payload(&answer, body))).await?;
while let Ok(Some(Observed::Delivery { event, .. })) = tokio::time::timeout(Duration::from_secs(30), inbox.recv()).await {
    // 이 채널로 오는 것은 모두 이 요청의 것. 끝 조건(예: 타입 "end")을 보고 구독을 푼다.
}
observer.unsubscribe(ruvent::Target::Channel(answer.as_str().into())).await?;
```

요청이 많은 프록시는 관찰자 대신 `ruvent-proxy`의 팬아웃을 쓴다. 요청 하나가 구성원 하나다(아래 "프록시").

워커 핸들러는 요청의 payload에서 응답 채널을 읽고, 조각은 `cx.emit_now(Envelope::dispatch(채널, 타입, 조각)).await`
(작업 연결로 바로 방출), 결과는 `cx.emit_dispatch(채널, 타입, 결과)`(완료와 한 스텝)로 낸다. 한 작업 연결이 순서를 지킨다.
결과도 발송이라 그 순간 구독자가 없으면 사라진다. 결과를 지킬지는 워커와 프로젝트가 정한다(계획 2.8). 관찰자(`observer::Observer`)는 구독을 연결마다 다시 내고 발송 엔벨롭을 그대로
넘긴다. 끊긴 동안의 발송은 `Observed::Gap`으로 알린다. 서버의 구독 확인은 `Observed::Subscribed { server, target, ticket }`로
스트림에 들어온다. ticket은 관찰자가 처리한 `subscribe` 호출의 순번이고, k번째 호출은 그 서버에서 ticket ≥ k인 첫 확인부터
효력이 있다(그 뒤의 발송은 구독이 선 뒤에 보내진 것).

| `Bye` 사유 | SDK |
| --- | --- |
| `superseded` | 그 생산자 id로 다시 붙지 않고 오류(`ProducerError::Refused`) |
| `draining` | 생산 후보에서 빼고, 최종 확인 뒤의 미확인분은 다른 서버로. 평소 백오프로 다시 붙고 `Welcome`을 받으면 후보에 넣는다 |
| `slow` | 다시 붙는다. 그사이 발송은 잃는다 |
| `version`, `token`, `forbidden` | 다시 붙지 않고 오류 |

소켓 한 줄만 쓰려면 `ruvent_client::remote::connect`(전송층, 재접속 없음)를 쓴다.

## 프록시

프록시는 프로젝트가 만든다. `ruvent-proxy`는 골조만 준다. 서버별 상향 관찰 연결과, 대상마다 상위 구독 하나를
구성원에게 나누는 팬아웃이다. 클라이언트 쪽 프로토콜(WebSocket, HTTP, WebRTC 등), 인증, 구독 권한 검사, 등록,
요청–응답, 기한, 역압, `/health`는 프로젝트가 만든다. 등록은 SDK의 생산자를 자기 설정으로 쓰고, 요청–응답은 요청마다
구성원 하나가 그 요청의 응답 채널을 구독하는 것으로 한다.

```rust,ignore
use ruvent_proxy::{Fanout, FanoutConfig, Target, Upstream};

let fanout = Fanout::start(FanoutConfig::default(), Upstream::new(tcp.clone(), servers.clone()));
let (member, mut events) = fanout.attach().await?;          // 클라이언트 연결 하나마다
member.subscribe(Target::Prefix("chat.room-7".into())).await?;  // 프로젝트의 권한 검사 뒤
while let Some(event) = events.recv().await { /* Arc<Envelope>를 클라이언트 프로토콜로 */ }
```

자세한 계약은 [crates/ruvent-proxy/README.md](../crates/ruvent-proxy/README.md).

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
