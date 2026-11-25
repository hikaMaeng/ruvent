# ruvent-proxy

최종 사용자용 프록시 서버를 만드는 라이브러리. 프로토콜별 프록시 크레이트나 배포용 바이너리는 따로 없다. 이 크레이트에 의존하는 작은 프로그램이 곧 프록시 서버다.

프록시는 이벤트 서버의 **발송 팬아웃 계층**이다(개편 계획 v4.6 4장). 서버 목록의 모든 서버에 붙고, 맡은 채널(또는 접두사)을 서버마다 한 번 구독해 받은 엔벨롭을 자기 사용자들에게 나눈다. 서버의 발송 비용은 사용자 수가 아니라 프록시 수에 비례한다.

* **프로토콜:** cargo feature로 고른다. 켠 프로토콜이 모두 한 `listen` 주소에서 함께 열린다.
* **도메인 규칙:** 훅으로 붙인다. 필요한 메서드만 쓰면 되고, 쓰지 않은 훅은 JSON 설정대로 동작한다.
* **상향 연결:** 클라이언트 SDK(`ruvent-client`)의 것이다. 재접속(백오프 + full jitter), 모든 소켓의 TCP keepalive(S13), `Bye` 사유표, 미확인 버퍼를 프록시가 따로 구현하지 않는다.

## 만드는 법

```toml
[dependencies]
ruvent-proxy = { path = "…", default-features = false, features = ["ws"] }   # ws, http 중 고른다. 기본은 둘 다
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

설정만으로 도는 프록시는 아래가 프로그램 전부다([examples/plain.rs](examples/plain.rs)).

```rust
#[tokio::main]
async fn main() -> Result<(), ruvent_proxy::LaunchError> {
    ruvent_proxy::Server::new().run().await   // my-proxy proxy.json
}
```

도메인 규칙은 `Hooks`를 구현해 `.hooks(…)`로 붙인다([examples/custom.rs](examples/custom.rs)). 훅은 클라이언트가 보낸 값을 확인한 뒤에야 믿는다. 아래 예는 쿠키의 세션 id를 프로그램 자신의 세션 표에서 찾는다. 쿠키 값을 그대로 사용자 이름으로 쓰면 아무 쿠키로나 들어올 수 있다.

```rust
struct Sessions(HashMap<String, Session>);   // 세션 id → 세션. 실제 프로그램은 세션 저장소에 묻는다

#[async_trait]
impl Hooks for Sessions {
    async fn authenticate(&self, cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
        let Some(id) = handshake.cookie("session") else {
            return cx.authenticate_by_config(handshake.token.as_deref()).await;   // 쿠키가 없으면 설정대로
        };
        let session = self.0.get(id).ok_or(Denied::Rejected)?;                    // 모르는 세션은 거절
        Ok(User::new(&session.user, [format!("tenant.{}.*", session.tenant)]).with_data(session.role))
    }
}

ruvent_proxy::Server::new().hooks(Sessions(sessions)).run().await
```

| feature | 경로 |
| --- | --- |
| 항상 | `GET /health`: 목록의 모든 서버에 대해 관찰 연결이 살아 있고 응답 채널 참여가 확인됐으면 200, 아니면 503. 본문은 서버별 상태 JSON(`Health`) |
| `ws` | `GET /v1/ws`: 사용자 하나에 WebSocket 하나. 텍스트 프레임에 사용자 JSON을 싣는다 |
| `http` | `POST /v1/events`(등록), `GET /v1/stream?channel=a&prefix=b`(SSE 구독) |

`Server`의 선택지:

| 메서드 | 뜻 | 없으면 |
| --- | --- | --- |
| `config(ProxyConfig)` / `config_file(path)` | 설정. 어느 쪽이든 `ProxyConfig::check`를 거친다 | 프로그램 첫 인자의 JSON 파일 |
| `hooks(H)` | 도메인 훅 | `Defaults`(모두 설정대로) |
| `upstream(Upstream)` | 상향 연결의 연결기·서버 목록·자격. 같은 프로세스 엔진(`Local`)이나 라우터가 관리하는 서버 목록(`ServerList`)을 쓸 때 | 설정의 `upstream.servers`에 `Tcp`로 |
| `run()` | 바인드, 허브 시작, 서비스. Ctrl-C에 끝남. Ctrl-C를 들을 수 없으면 `LaunchError::Signal` | — |
| `run_until(signal)` | `run()`과 같고, `signal`이 끝나면 멈춤 | — |
| `bind()` | 바인드와 허브 시작까지만. `Bound::serve()` 또는 `Bound::serve_until(signal)`로 서비스 | — |

멈추면 허브도 멈춘다. 모든 사용자 스트림이 끝나고(WS는 소켓이 닫히고 SSE는 응답이 끝난다), 모든 상향 연결을 놓는다. 허브만 멈추려면 `Proxy::stop()`을 부른다. 모든 `Proxy`와 사용자 핸들이 사라져도 허브는 끝난다.

자기 `axum` 앱에 경로를 더하거나 프록시를 그 안에 넣으려면 `ruvent_proxy::router(proxy)`를 합친다.

## 상향 연결

`Upstream { connector, servers, credential }`이 정한다.

| 필드 | 뜻 |
| --- | --- |
| `connector` | 연결을 여는 방법. `Tcp::new(token)`(실제 서버, keepalive는 SDK 기본), `Local`(같은 프로세스의 엔진) |
| `servers` | 서버 목록 공급자(`ServerList`). `Servers`는 코드에서 정하고 `set`으로 바꾸는 목록이다. 목록이 바뀌면 프록시가 새 서버에 붙고 빠진 서버를 놓는다 |
| `credential` | 같은 프로세스 연결의 자격 이름(기본 `proxy`). TCP에서는 토큰이 정한다 |

서버마다 역할별 연결이 하나씩이다.

| 역할 | 쓰는 것 | 하는 일 |
| --- | --- | --- |
| 관찰 | SDK `Observer`, 허브 태스크 | 사용자들이 원하는 대상(채널, 접두사)을 서버마다 하나씩 구독한다. 발송을 `Envelope` 그대로 받는다 |
| 생산 | SDK `Producer`(요청자의 것) | 사용자 등록. 서버마다 순번과 누적 확인(2.3) |
| 작업 | SDK `Requester` | 프록시 인스턴스의 응답 채널 `reply.<인스턴스 id>`에 서버마다 접속 때 한 번 참여(발송 포함). 결과와 조각을 받는다 |

## 훅

| 훅 | 불리는 때 | 기본 동작(공개 함수로도 있음) |
| --- | --- | --- |
| `authenticate(cx, handshake) -> Result<User, Denied>` | HTTP 요청마다 한 번. WebSocket마다 한 번, 첫 답이 `Missing`이고 토큰이 첫 메시지로 오면 두 번 | `auth.mode`(`Context::authenticate_by_config`) |
| `subscribe(cx, user, target) -> Result<(), Rejection>` | 사용자 구독을 상향으로 보내기 전. `target`은 `Target::Channel` 또는 `Target::Prefix` | 사용자 범위가 대상을 덮는가(`hooks::within_scope`, `User::covers`) |
| `publish(cx, user, &mut publish) -> Result<(), Rejection>` | 요청 자체의 모양 검사 뒤, 등록 전. 내용을 바꿀 수 있다 | 채널과 reply 채널의 범위(`hooks::publish_within_scope`) |
| `deliver(user, event) -> bool` | 허브 안에서 이벤트마다, 그 이벤트를 받는 구독의 사용자마다 | 모두 통과 |

* **`Handshake`:** 헤더, 원 쿼리, TCP 상대 주소, 프로토콜이 찾은 `token`을 담는다. WS는 `?token=`(첫 값. 빈 값도 토큰이다) 또는 첫 `{"op":"auth"}` 메시지, HTTP는 `Authorization: Bearer`(방식 이름은 대소문자 무관, 빈 토큰은 없음)에서 찾는다. 읽기 도우미는 `header`, `bearer`, `cookie`, `query_value`다. 쿼리는 키와 값을 모두 디코딩한다. `peer`는 TCP 상대라서, 역방향 프록시 뒤에서는 그 프록시의 주소다.
* **`Denied::Missing`:** 자격 증명이 없다는 뜻이다. WS는 이 답을 받고 토큰도 없으면 첫 메시지로 토큰을 받아 한 번 더 인증한다. HTTP는 401로 답한다.
* **`User`:** `name`은 등록의 `delegate`로 실린다. `scope`는 기본 훅이 보는 채널 범위(정확한 이름 또는 `*`로 끝나는 패턴)다. 접두사 대상 `p`는 `p.`로 시작하는 모든 채널을 패턴 하나가 덮을 때만 허용한다(`room*`·`room.*`는 `room`을 덮고, 정확한 이름 `room`은 접두사를 덮지 않는다). 그 밖에 도메인이 아는 값은 `with_data`로 붙이고 `data::<T>()`로 꺼낸다. `Debug` 출력에는 그 값이 나오지 않는다.
* **`Rejection { cause, message }`:** 사용자에게는 `message`가 간다. HTTP 상태는 `cause`로 정한다. `Invalid` 400, `Forbidden` 403, `Conflict` 409, `Unavailable` 503. 변환은 아래 표.
* **`Context`:** `config()`, `upstream()`, `authenticate_by_config()`를 준다. `upstream()`은 `UpstreamCalls`이고 프록시의 서비스 자격으로 `send`·`send_committed`·`delete`(생산)와 `exchange`·`exchange_stream`(요청–응답)만 부른다. 구독·취소는 없다. 허브는 자기 관찰자의 구독 확인(ticket)으로 사용자 가입이 언제 효력을 갖는지 정하는데, 다른 곳이 같은 관찰자로 구독하면 그 확인을 자기 것으로 센다. 자기 구독이 필요한 훅은 자기 관찰자를 연다.
* **`publish`의 순서:** 먼저 요청 자체의 모양(`id`·`correlation`이 UUID인지)을 본다(400). 그다음 훅이 돌고, 훅이 남긴 그대로 등록한다. 그러므로 **재작성하는 훅은 재작성한 뒤에 검사한다.** 채널을 바꿨다면 바뀐 채널과 `reply`를 `publish_within_scope`로 다시 본다. `Publish::payload_as::<T>()`로 읽고 `set_payload(&value)`로 바꾼다.
* **`deliver`는 동기 함수다.** 허브의 단일 태스크에서 이벤트 하나당 사용자 수만큼 불린다. 여기서 기다리면 프록시의 모든 사용자가 함께 기다린다. 비동기 판단은 `subscribe`와 `publish`에서 한다. `deliver`가 패닉하면 허브가 잡는다. 그 사용자는 이벤트를 놓쳤으므로 끊고, 허브와 다른 사용자는 계속 돈다.
* **범위 검사는 기본 훅의 일이다.** `publish`를 직접 쓰면서 `publish_within_scope`를 부르지 않으면 채널 범위 검사가 빠진다.

### 오류 → 원인

| 출처 | 변환 | 원인 |
| --- | --- | --- |
| 응답이 있는 호출의 `Refusal`(구독 등) | `Rejection::from_refusal` | `Forbidden` → Forbidden, `Missing`·`AlreadyClaimed` → Conflict, `Malformed` → Invalid, `Closed` → Unavailable |
| `FaultKind`(연결·종류마다 첫 1회, 2.7) | `Rejection::from_fault` | `Forbidden` → Forbidden, `Malformed`·`InvalidId` → Invalid, `IdConflict`·`FollowUpConflict` → Conflict, `SequenceGap` → Unavailable |
| 생산자 오류(`ProducerError`) | `Rejection::from_producer` | 서버가 토큰·권한으로 끊음 → Forbidden, 나머지(`superseded`, `draining` 등, 재시도 예산, 닫힘) → Unavailable |
| 요청–응답 오류(`RequestError`) | `Rejection::from_request` | 기한 → Unavailable, 생산자 오류 → 위 표, 닫힘 → Unavailable |

등록에는 건별 응답이 없다(2.3). 같은 id·같은 내용의 재등록은 서버가 조용히 버리고(재전송), 상향 자격이 쓸 수 없는 채널이나 id 충돌은 프록시의 연결에 `Fault`로 한 번 알려질 뿐 사용자에게는 가지 않는다. 사용자를 돌려보내는 것은 프록시 자신의 검사(모양, 훅)다.

## 모듈

| 모듈 | 내용 |
| --- | --- |
| `hooks` | `Hooks`, `Defaults`(`mod.rs`), `Context`, `UpstreamCalls`(`context.rs`), 기본 규칙 함수(`rules.rs`) |
| `server` | `Server`, `Bound`, `LaunchError` (feature `ws` 또는 `http`) |
| `protocol` | `router`, `protocol::ws`, `protocol::http` |
| `handshake` | `Handshake`, 쿼리 해석 |
| `config` | JSON 설정과 `check`. 예시: [proxy.example.json](proxy.example.json) |
| `auth` | `User`(`covers`), `Denied`, 설정의 인증 모드: `none`, `static`(Q1(c)), `worker`(Q1(a)) |
| `hub` | `mod.rs`: `Proxy`(시작, 준비, 사용자, 인증). `calls.rs`: 등록(`publish`)과 요청–응답(`exchange`). `user.rs`: `UserHandle`. `channels/`: 구독 장부(순수 상태, 태스크·시계 없음). `task.rs`: 허브 태스크(장부의 단계 실행, 팬아웃, 상태 보고) |
| `health` | `/health`가 보고하는 것: 서버별 관찰·응답 연결과 준비 여부(순수 계산) |
| `message` | 사용자 JSON 형식, `Publish`(모양 검사, 등록 변환), `Outcome`, `Outgoing`(원 엔벨롭 스트림), `Rejection`, `Cause` |
| `upstream` | `Upstream`과 SDK의 `Connector`·`Tcp`·`Local`·`ServerList`·`Servers` 재수출 |

역할 분리는 정적 검사 `the_proxy_keeps_its_roles_apart`(`crates/ruvent/tests/static/architecture.rs`)가 지킨다. 장부에 태스크·큐·시계가 들어오거나, 훅 핸들에 구독이 생기거나, `deliver`가 패닉 방어 밖에서 불리면 실패한다.

## 설정

| 키 | 뜻 | 기본값 |
| --- | --- | --- |
| `listen` | 프록시가 듣는 주소. 켠 프로토콜이 모두 여기서 열린다 | 필수 |
| `upstream.servers` | 이벤트 서버 주소 목록. 프록시는 모두에 붙는다. 비면 거절 | 필수 |
| `upstream.token` | 이 프록시의 서비스 자격 토큰(모든 서버 공통) | 필수 |
| `auth.mode` | `none` / `static` / `worker`. 기본 `authenticate` 훅이 쓴다 | `none` |
| `auth.tokens` | `static`용 `[{token, user, channels}]`. `channels`가 비면 기본 채널 | `[]` |
| `auth.worker` | `worker`용 `{channel, type, timeout_ms}`. `timeout_ms` ≥ 1. 응답 채널은 프록시 인스턴스의 것이라 적지 않는다 | — |
| `channels` | 인증이 따로 정하지 않을 때 사용자가 쓸 수 있는 채널. 정확한 이름 또는 `*`로 끝나는 접두사 | `["*"]` |
| `user_buffer` | 사용자 송신 버퍼 메시지 수(≥ 1). 가득 차면 그 사용자를 끊는다(손실 신호) | `1024` |
| `delegate` | 등록에 최종 사용자를 `delegate`로 싣는다. 상향 자격에 위임 권한이 있어야 한다 | `true` |
| `reconnect.initial_ms`, `reconnect.max_ms`, `reconnect.draining_ms` | 상향 재접속 백오프(S20): 첫 상한, 상한, `Bye{draining}` 뒤 생산 연결의 상한. `1 ≤ initial_ms ≤ max_ms ≤ draining_ms` | `50`, `1000`, `30000` |
| `exchange.deadline_ms` | `Proxy::exchange`의 기본 기한(호출 대기와 요청의 `expires`) | `30000` |
| `exchange.lease_ms` | 응답 채널 결과 배정의 lease(S16) | `5000` |

모르는 키는 거절한다(옛 `upstream.address`, `auth.worker.reply_channel` 포함).

## 동작

* **구독 집약.** 이벤트 서버 쪽 구독은 대상(채널 또는 접두사)마다, 서버마다 하나다. `deliver` 훅이 사용자별로 걸러도 상향 전달은 서버마다 한 번이다.
* **효력 시점(사용자 구독의 불변식).** 사용자가 새로 가입할 때마다 프록시는 관찰자에 구독을 한 번 더 낸다(서버는 같은 연결의 중복 구독을 하나로 두고 확인만 다시 보낸다). 관찰자는 `subscribe` 호출에 처리 순서대로 ticket을 붙이고, 각 서버의 확인을 그 ticket과 함께 스트림에 넣는다(`Observed::Subscribed`). 허브는 그 사용자의 ticket 이상인 확인을 **그 서버에서** 읽은 뒤에야 그 서버의 발송을 그 사용자에게 준다. 확인 이전에 받은 발송은 이미 활성이던 사용자에게만 간다. 그래서 늦게 붙은 사용자나 `0→1→0→1` 재구독에 옛 이벤트가 섞이지 않는다. 가입은 구독 호출이 거절 없이 돌아오고 한 서버 이상에서 효력이 생기면 답한다. 이미 답을 받은 같은 사용자의 재가입은 호출 없이 즉시 성공한다. 붙은 서버가 하나도 없을 때의 가입은 첫 서버가 붙어 확인할 때 답한다.
* **접두사 구독.** `Target::Prefix("room")`은 `room.` 아래의 모든 채널(깊이 무관)을 받는다. `room` 자체와 `roomy`는 받지 않는다. 범위 패턴의 `starts_with`(`room*`는 `roomy`를 허용)와 다르다.
* **마지막 사용자가 떠나면** 상향 구독을 취소한다. 구독과 취소는 작업자 하나가 장부가 낸 순서대로 관찰자에 넘긴다. 확인을 기다리던 가입이 그 사이에 떠나면 "unsubscribed before the subscription took effect"로 답한다.
* **떠난 사용자는** 다음 전달 때 정리된다. `deliver` 훅이 그 사용자를 걸러도 마찬가지다.
* **느린 사용자는 프록시가 끊는다.** 사용자 송신 버퍼가 차면 그 사용자를 끊는다. 서버는 프록시 연결 하나만 본다. 프록시 자신이 느려 서버의 미송신 상한(S7)을 넘으면 서버가 `Bye{slow}`로 끊고, SDK가 다시 붙어 구독을 다시 낸다. 그사이의 발송은 잃는다(1회성).
* **상향이 끊기면** SDK가 백오프로 재접속하고 관찰자가 구독을 모두 다시 낸다. 다시 붙은 연결에서는 그 서버의 확인을 읽은 뒤부터 다시 전달한다. 끊긴 동안의 발송은 잃는다.
* **`/health`.** 목록의 모든 서버에서 관찰 연결이 살아 있고(`Up` ~ `Gap`) 응답 채널 참여가 확인돼야 200이다. FIN 없이 조용히 끊긴 연결은 SDK가 건 커널 keepalive(S13, 약 30초)가 찾아 닫으므로 정상으로 남지 않는다.
* **원 엔벨롭 스트림.** `Proxy::attach_raw`로 붙은 사용자는 `Outgoing::Event(Arc<Envelope>)`를 받는다. 허브가 엔벨롭 하나를 `Arc`로 나누고 payload를 JSON 텍스트로 바꾸지 않는다. `attach`(WS·HTTP)는 지금처럼 JSON 텍스트를 받고, 그 텍스트는 이벤트마다 한 번 만든다.
* **등록(`Proxy::publish`).** id가 없으면 프록시가 UUIDv7을 붙인다(답의 `id`가 이벤트의 id다). 선택기가 고른 서버의 생산 연결로 보내고 그 서버의 누적 `Committed`까지 기다린 뒤 답한다. 붙은 서버가 하나도 없으면 기다리지 않고 503이다.
* **요청–응답(`Proxy::exchange`).** SDK 요청자로 한다. 요청에 프록시 인스턴스의 응답 채널과 새 `correlation`을 싣는다. 종결 결과는 응답 채널의 착수용 이벤트이고, 요청마다 구독하거나 참여하지 않는다. 스트림 조각은 같은 작업 연결로 결과보다 앞서 온다(`exchange_stream`). 대기자는 결과, 취소(`Stream` 버림), 기한 중 먼저 온 것으로 끝난다. **상향 연결이 끊겨도 끝나지 않는다.** 결과는 서버에서 기다리다 다시 붙으면 배정된다. 그사이 조각을 잃었으면 `fragments_lost`가 선다. 기한이 지난 요청은 `expires`로 서버의 대기열에서도 사라진다. 이미 착수된 요청은 끝까지 가고, 그 결과는 결과 보관 기한까지 기다린다.
* **인증 워커(Q1(a)).** 위 요청–응답 하나다. `auth.worker.channel`에 payload `{"token":…}`인 착수용 이벤트를 보내고, 워커는 그것을 완료하면서 요청의 `reply` 채널에 같은 correlation으로 **착수용** 후속 `{"ok":true|false,"user":"…","channels":["…"]}`를 낸다(SDK 워커의 `Context::reply`). `timeout_ms` 안에 답이 없으면 `Denied::Timeout`.
* **HTTP는 요청마다 따로 인증한다.** SSE 유지용 주기 핑은 보내지 않는다(타이머 없음). 빈 `channel=`·`prefix=`는 건너뛴다.

## 타이머

이 크레이트는 시계를 기다리지 않는다. 재접속 백오프는 SDK의 `line`, 인증 워커와 `exchange`의 기한은 SDK 요청자의 호출 기한이 맡는다(`crates/ruvent/tests/static/architecture.rs`의 `TIMERS`).

## 사용자 JSON

`src/message/mod.rs` 머리 주석이 원본이다.

```json
{"op":"auth","token":"…"}
{"op":"publish","kind":"claimable|dispatch","channel":"…","type":"…","payload":{},"id":"<uuid>","reply":"…","correlation":"<uuid>","ref":1}
{"op":"subscribe","channel":"…","ref":1}
{"op":"subscribe","prefix":"…","ref":1}
{"op":"unsubscribe","channel":"…"|"prefix":"…","ref":1}
→ {"op":"event","channel":"…","type":"…","id":"<uuid>","payload":{},"correlation":"<uuid>"}
→ {"op":"result","ref":1,"ok":true,"id":"<uuid>","error":"…"}
```

## 테스트

`cargo test -p ruvent-proxy`. 범주별 테스트 이름은 [docs/testing.md](../../docs/testing.md)의 "프록시" 절에 있다.

| 위치 | 판정 |
| --- | --- |
| `src/**/tests.rs` | 단위: 구독 장부(ticket, 서버별 효력, 접두사), 사용자 JSON과 등록 변환, 오류 → 원인, 핸드셰이크, 설정, 인증 모드와 범위, 기본 규칙, 상태 코드, health 계산, 서버 조립기 |
| `tests/hub.rs` | 같은 프로세스의 엔진(SDK `Local`): 집약, 효력 시점, 재접속과 재구독, 목록에 더한 서버, 차단과 정리, 원 엔벨롭, 위임, 인증 워커, `exchange` 기한, 허브 안의 훅, 정지와 상향 해제 |
| `tests/ws.rs`, `tests/http.rs` | 실제 TCP로 각 프로토콜의 세션, 토큰, 상태 코드, health 본문, SSE 접두사, 같은 id 재등록, 정지 |
| `tests/hooks.rs` | `Server`로 만든 프록시의 훅: 쿠키 인증과 사용자 데이터, 구독·등록 훅, 재작성 뒤 reply 검사, 인증 호출 횟수, 사용자별 `deliver` |
| `tests/acceptance.rs` | 실제 `ruvent-server`에 TCP로 붙인 인수: 예제 `plain`을 별도 프로세스로, 훅 하나로 바꾼 프록시, 효력 시점 불변식, 접두사 구독, 서버 2대, 상향 상실과 `/health`, 조각 → 결과 순서, 재접속을 넘는 `exchange`, 정지 신호 |

feature 조합(`ws`만, `http`만, 없음)은 `cargo xtask verify`가 각각 린트한다. 예제는 어느 조합에서도 빌드된다(프로토콜이 없으면 빈 `main`).
