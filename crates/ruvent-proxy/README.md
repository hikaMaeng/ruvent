# ruvent-proxy

최종 사용자용 프록시 서버를 만드는 라이브러리. 프로토콜별 프록시 크레이트나 배포용 바이너리는 따로 없다. 이 크레이트에 의존하는 작은 프로그램이 곧 프록시 서버다(제안서 4.3, 6.5, 6.8, 13·14장).

* **프로토콜:** cargo feature로 고른다. 켠 프로토콜이 모두 한 `listen` 주소에서 함께 열린다.
* **도메인 규칙:** 훅으로 붙인다. 필요한 메서드만 쓰면 되고, 쓰지 않은 훅은 JSON 설정대로 동작한다.

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
| 항상 | `GET /health`: 상위 연결이 살아 있고 허브가 돌면 200, 아니면 503 |
| `ws` | `GET /v1/ws`: 사용자 하나에 WebSocket 하나. 텍스트 프레임에 사용자 JSON을 싣는다 |
| `http` | `POST /v1/events`(등록), `GET /v1/stream?channel=a&channel=b`(SSE 구독) |

`Server`의 선택지:

| 메서드 | 뜻 | 없으면 |
| --- | --- | --- |
| `config(ProxyConfig)` / `config_file(path)` | 설정. 어느 쪽이든 `ProxyConfig::check`를 거친다 | 프로그램 첫 인자의 JSON 파일 |
| `hooks(H)` | 도메인 훅 | `Defaults`(모두 설정대로) |
| `upstream(C)` | 상위 연결 공급자(`InProcess` 등) | 설정의 `upstream`으로 원격 접속(`Remote`) |
| `run()` | 바인드, 허브 시작, 서비스. Ctrl-C에 끝남. Ctrl-C를 들을 수 없으면 `LaunchError::Signal` | — |
| `run_until(signal)` | `run()`과 같고, `signal`이 끝나면 멈춤 | — |
| `bind()` | 바인드와 허브 시작까지만. `Bound::serve()` 또는 `Bound::serve_until(signal)`로 서비스 | — |

멈추면 허브도 멈춘다. 모든 사용자 스트림이 끝나고(WS는 소켓이 닫히고 SSE는 응답이 끝난다), 상위 연결을 놓는다. 허브만 멈추려면 `Proxy::stop()`을 부른다. 모든 `Proxy`와 사용자 핸들이 사라져도 허브는 끝난다.

자기 `axum` 앱에 경로를 더하거나 프록시를 그 안에 넣으려면 `ruvent_proxy::router(proxy)`를 합친다.

## 훅

| 훅 | 불리는 때 | 기본 동작(공개 함수로도 있음) |
| --- | --- | --- |
| `authenticate(cx, handshake) -> Result<User, Denied>` | HTTP 요청마다 한 번. WebSocket마다 한 번, 첫 답이 `Missing`이고 토큰이 첫 메시지로 오면 두 번 | `auth.mode`(`Context::authenticate_by_config`) |
| `subscribe(cx, user, channel) -> Result<(), Rejection>` | 사용자 구독을 상위로 보내기 전 | 사용자 채널 범위(`hooks::within_scope`) |
| `publish(cx, user, &mut publish) -> Result<(), Rejection>` | 요청 자체의 모양 검사 뒤, 등록 전. 내용을 바꿀 수 있다 | 채널과 reply 채널의 범위(`hooks::publish_within_scope`) |
| `deliver(user, event) -> bool` | 허브 안에서 이벤트마다, 그 채널의 사용자마다 | 모두 통과 |

* **`Handshake`:** 헤더, 원 쿼리, TCP 상대 주소, 프로토콜이 찾은 `token`을 담는다. WS는 `?token=`(첫 값. 빈 값도 토큰이다) 또는 첫 `{"op":"auth"}` 메시지, HTTP는 `Authorization: Bearer`(방식 이름은 대소문자 무관, 빈 토큰은 없음)에서 찾는다. 읽기 도우미는 `header`, `bearer`, `cookie`, `query_value`다. 쿼리는 키와 값을 모두 디코딩한다. `peer`는 TCP 상대라서, 역방향 프록시 뒤에서는 그 프록시의 주소다.
* **`Denied::Missing`:** 자격 증명이 없다는 뜻이다. WS는 이 답을 받고 토큰도 없으면 첫 메시지로 토큰을 받아 한 번 더 인증한다. HTTP는 401로 답한다.
* **`User`:** `name`은 등록의 `delegate`로 실린다. `scope`는 기본 훅이 보는 채널 범위다. 그 밖에 도메인이 아는 값(테넌트, 역할, 세션)은 `with_data`로 붙이고 `data::<T>()`로 꺼낸다. `Debug` 출력에는 그 값이 나오지 않는다.
* **`Rejection { cause, message }`:** 사용자에게는 `message`가 간다. HTTP 상태는 `cause`로 정한다. `Invalid`는 400, `Forbidden`은 403, `Conflict`는 409, `Unavailable`은 503이다.
* **`Context`:** `config()`, `upstream()`, `authenticate_by_config()`를 준다. `upstream()`은 `UpstreamCalls`이고 프록시의 서비스 자격으로 등록·착수·완료·삭제만 부른다. 구독·취소는 없다. 허브는 자기 연결의 `Subscribed` 표지를 세어 사용자 가입이 언제 효력을 갖는지 정하는데, 같은 연결로 다른 곳이 구독하면 그 표지를 자기 것으로 센다. 자기 구독이 필요한 훅은 자기 연결을 연다.
* **`publish`의 순서:** 먼저 요청 자체의 모양(`id`·`correlation`이 UUID인지)을 본다(400). 그다음 훅이 돌고, 훅이 남긴 그대로 등록한다. 그러므로 **재작성하는 훅은 재작성한 뒤에 검사한다.** 채널을 바꿨다면 바뀐 채널과 `reply`를 `publish_within_scope`로 다시 본다. `Publish::payload_as::<T>()`로 읽고 `set_payload(&value)`로 바꾼다.
* **`deliver`는 동기 함수다.** 허브의 단일 태스크에서 이벤트 하나당 사용자 수만큼 불린다. 여기서 기다리면 프록시의 모든 사용자가 함께 기다린다. 비동기 판단은 `subscribe`와 `publish`에서 한다. 이 둘은 사용자 자신의 태스크에서 돌므로 기다려도 그 사용자만 늦어진다. `deliver`가 패닉하면 허브가 잡는다. 그 사용자는 이벤트를 놓쳤으므로 끊고, 허브와 다른 사용자는 계속 돈다.
* **범위 검사는 기본 훅의 일이다.** `publish`를 직접 쓰면서 `publish_within_scope`를 부르지 않으면 채널 범위 검사가 빠진다.

## 모듈

| 모듈 | 내용 |
| --- | --- |
| `hooks` | `Hooks`, `Defaults`(`mod.rs`), `Context`, `UpstreamCalls`(`context.rs`), 기본 규칙 함수(`rules.rs`) |
| `server` | `Server`, `Bound`, `LaunchError` (feature `ws` 또는 `http`) |
| `protocol` | `router`, `protocol::ws`, `protocol::http` |
| `handshake` | `Handshake`, 쿼리 해석 |
| `config` | JSON 설정과 `check`. 예시: [proxy.example.json](proxy.example.json) |
| `auth` | `User`, `Denied`, 설정의 인증 모드: `none`, `static`(Q1(c)), `worker`(Q1(a)) |
| `hub` | `mod.rs`: `Proxy`·`UserHandle`(호출자가 쥐는 것). `channels/`: 구독 장부(순수 상태, 태스크·시계 없음). `task.rs`: 허브 태스크(장부의 단계 실행, 팬아웃, 재접속) |
| `message` | 사용자 JSON 형식, `Publish`(모양 검사, 등록 변환), `Outcome`, `Rejection`, `Cause` |
| `upstream` | 상위 연결 공급자. `Remote`(이진 프로토콜), `InProcess`(같은 프로세스의 엔진) |

역할 분리는 정적 검사 `the_proxy_keeps_its_roles_apart`(`crates/ruvent/tests/static/architecture.rs`)가 지킨다. 장부에 태스크·큐·시계가 들어오거나, 훅 핸들에 구독이 생기거나, `deliver`가 패닉 방어 밖에서 불리면 실패한다.

## 설정

| 키 | 뜻 | 기본값 |
| --- | --- | --- |
| `listen` | 프록시가 듣는 주소. 켠 프로토콜이 모두 여기서 열린다 | 필수 |
| `upstream.address`, `upstream.token` | 이벤트 서버 주소와 이 프록시의 서비스 자격 토큰 | 필수 |
| `auth.mode` | `none` / `static` / `worker`. 기본 `authenticate` 훅이 쓴다 | `none` |
| `auth.tokens` | `static`용 `[{token, user, channels}]`. `channels`가 비면 기본 채널 | `[]` |
| `auth.worker` | `worker`용 `{channel, type, reply_channel, timeout_ms}`. `timeout_ms` ≥ 1 | — |
| `channels` | 인증이 따로 정하지 않을 때 사용자가 쓸 수 있는 채널. 정확한 이름 또는 `*`로 끝나는 접두사 | `["*"]` |
| `user_buffer` | 사용자 송신 버퍼 메시지 수(≥ 1). 가득 차면 그 사용자를 끊는다(손실 신호) | `1024` |
| `delegate` | 등록에 최종 사용자를 `delegate`로 싣는다. 상위 자격에 위임 권한이 있어야 한다 | `true` |
| `reconnect.initial_ms`, `reconnect.max_ms` | 상위 재접속 백오프의 첫 대기와 상한. `1 ≤ initial_ms ≤ max_ms` | `100`, `5000` |

모르는 키는 거절한다.

## 동작

* **구독 집약.** 이벤트 서버 쪽 구독은 채널마다 하나다. 사용자가 가입할 때마다 프록시는 상위에 구독을 한 번 더 보내고(서버는 같은 연결의 중복 구독을 하나로 둔다), 그 응답의 `Subscribed` 표지를 읽은 뒤에 그 사용자를 활성으로 만든다. 표지 이전에 받은 이벤트는 이미 활성이던 사용자에게만 간다. 그래서 늦게 붙은 사용자나 `0→1→0→1` 재구독에 옛 이벤트가 섞이지 않는다. 상위가 연결되기 전에 한 가입은 연결된 뒤 보낸 구독의 표지로 효력을 갖는다. `deliver` 훅이 사용자별로 걸러도 상위 전달은 채널당 한 번이다.
* **마지막 사용자가 떠나면** 상위 구독을 취소한다. 구독과 취소는 연결마다 작업자 하나가 낸 순서대로 보낸다. 표지를 기다리던 가입이 그 사이에 떠나면 "unsubscribed before the subscription took effect"로 답한다.
* **떠난 사용자는** 다음 전달 때 정리된다. `deliver` 훅이 그 사용자를 걸러도 마찬가지다.
* **상위가 끊기면** 백오프로 재접속하고, 사용자가 남아 있는 채널을 모두 다시 구독한다. 끊긴 동안의 발송은 잃는다(Q8(a)).
* **인증 워커(Q1(a)).** 프록시는 `auth.worker.channel`에 착수용 이벤트를 등록한다. payload는 `{"token":…}`, correlation은 무작위 값, `reply`는 `auth.worker.reply_channel`이다. 인증 워커는 그 이벤트를 완료하면서, 같은 correlation으로 reply 채널에 발송용 후속 `{"ok":true|false,"user":"…","channels":["…"]}`를 낸다. `timeout_ms` 안에 답이 없으면 거절한다.
* **HTTP는 요청마다 따로 인증한다.** SSE 유지용 주기 핑은 보내지 않는다(타이머 없음). 빈 `channel=`은 건너뛴다.

## 타이머

이 크레이트가 시계를 기다리는 곳은 둘이다.
* 상위 재접속 백오프(`hub/task.rs`)
* 인증 워커 답의 기한(`hub/mod.rs`, `Proxy::authenticate_by_config`)

## 사용자 JSON

`src/message/mod.rs` 머리 주석이 원본이다.

```json
{"op":"auth","token":"…"}
{"op":"publish","kind":"claimable|dispatch","channel":"…","type":"…","payload":{},"id":"<uuid>","reply":"…","correlation":"<uuid>","ref":1}
{"op":"subscribe","channel":"…","ref":1}
{"op":"unsubscribe","channel":"…","ref":1}
→ {"op":"event","channel":"…","type":"…","id":"<uuid>","payload":{},"correlation":"<uuid>"}
→ {"op":"result","ref":1,"ok":true,"id":"<uuid>","error":"…"}
```

## 테스트

`cargo test -p ruvent-proxy`. 범주별 테스트 이름은 [docs/testing.md](../../docs/testing.md)의 "프록시" 절에 있다.

| 위치 | 판정 |
| --- | --- |
| `src/**/tests.rs` | 단위: 구독 장부, 사용자 JSON과 등록 변환, 거절 원인, 핸드셰이크, 설정, 인증 모드, 기본 규칙, 상태 코드, 서버 조립기 |
| `tests/hub.rs` | 허브와 같은 프로세스의 엔진: 집약, 효력 시점, 차단과 정리, 위임, 인증, 허브 안의 훅(패닉 포함), 정지 |
| `tests/ws.rs`, `tests/http.rs` | 실제 TCP로 각 프로토콜의 세션, 토큰, 상태 코드, health와 정지 |
| `tests/hooks.rs` | `Server`로 만든 프록시의 훅: 쿠키 인증과 사용자 데이터, 구독·등록 훅, 재작성 뒤 reply 검사, 인증 호출 횟수, 사용자별 `deliver` |
| `tests/acceptance.rs` | 실제 `ruvent-server`에 TCP로 붙인 인수: 예제 `plain`을 별도 프로세스로 실행(argv 설정, `Remote`), 훅 하나로 바꾼 프록시, 효력 시점 불변식, 정지 신호 |

feature 조합(`ws`만, `http`만, 없음)은 `cargo xtask verify`가 각각 린트한다. 예제는 어느 조합에서도 빌드된다(프로토콜이 없으면 빈 `main`).
