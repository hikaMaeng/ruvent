# ruvent-proxy

최종 사용자용 프록시 서버를 만드는 라이브러리. 프로토콜별 프록시 크레이트나 배포용 바이너리는 따로 없다. 이 크레이트에 의존하는 작은 프로그램이 곧 프록시 서버다(제안서 4.3, 6.5, 6.8, 14장).

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

도메인 규칙은 `Hooks`를 구현해 `.hooks(…)`로 붙인다([examples/custom.rs](examples/custom.rs)).

```rust
struct Domain;

#[async_trait]
impl Hooks for Domain {
    async fn authenticate(&self, cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
        match handshake.cookie("session") {
            Some(session) => Ok(User::new(session, ["chat.*"]).with_data(Tenant::of(session))),
            None => cx.authenticate_by_config(handshake.token.as_deref()).await,   // 나머지는 설정대로
        }
    }
}

ruvent_proxy::Server::new().hooks(Domain).run().await
```

| feature | 경로 |
| --- | --- |
| 항상 | `GET /health`: 상위 연결이 살아 있으면 200, 아니면 503 |
| `ws` | `GET /v1/ws`: 사용자 하나에 WebSocket 하나. 텍스트 프레임에 사용자 JSON을 싣는다 |
| `http` | `POST /v1/events`(등록), `GET /v1/stream?channel=a&channel=b`(SSE 구독) |

`Server`의 선택지:

| 메서드 | 뜻 | 없으면 |
| --- | --- | --- |
| `config(ProxyConfig)` / `config_file(path)` | 설정 | 프로그램 첫 인자의 JSON 파일 |
| `hooks(H)` | 도메인 훅 | `Defaults`(모두 설정대로) |
| `upstream(C)` | 상위 연결 공급자(`InProcess` 등) | 설정의 `upstream`으로 원격 접속 |
| `run()` | 바인드, 허브 시작, 서비스. Ctrl-C에 끝남 | — |
| `bind()` | 바인드와 허브 시작까지만. `Bound::serve()`로 서비스 | — |

자기 `axum` 앱에 경로를 더하거나 프록시를 그 안에 넣으려면 `ruvent_proxy::router(proxy)`를 합친다.

## 훅

| 훅 | 불리는 때 | 기본 동작(공개 함수로도 있음) |
| --- | --- | --- |
| `authenticate(cx, handshake) -> Result<User, Denied>` | WebSocket마다 한 번, HTTP 요청마다 한 번 | `auth.mode`(`Context::authenticate_by_config`) |
| `subscribe(cx, user, channel) -> Result<(), Rejection>` | 사용자 구독을 상위로 보내기 전 | 사용자 채널 범위(`hooks::within_scope`) |
| `publish(cx, user, &mut publish) -> Result<(), Rejection>` | 사용자 등록 전. 내용을 바꿀 수 있다 | 채널과 reply 채널의 범위(`hooks::publish_within_scope`) |
| `deliver(user, event) -> bool` | 허브 안에서 이벤트마다, 그 채널의 사용자마다 | 모두 통과 |

* **`Handshake`:** 헤더, 원 쿼리, 상대 주소, 프로토콜이 찾은 `token`을 담는다. WS는 `?token=` 또는 첫 `{"op":"auth"}` 메시지, HTTP는 `Authorization: Bearer`에서 찾는다. 읽기 도우미는 `header`, `bearer`, `cookie`, `query_value`.
* **`Denied::Missing`:** 자격 증명이 없다는 뜻이다. WS는 이 답을 받고 토큰도 없으면 첫 메시지로 토큰을 받아 한 번 더 인증한다. HTTP는 401로 답한다.
* **`User`:** `name`은 등록의 `delegate`로 실린다. `scope`는 기본 훅이 보는 채널 범위다. 그 밖에 도메인이 아는 값(테넌트, 역할, 세션)은 `with_data`로 붙이고 `data::<T>()`로 꺼낸다.
* **`Rejection { cause, message }`:** 사용자에게는 `message`가 간다. HTTP 상태는 `cause`로 정한다. `Invalid`는 400, `Forbidden`은 403, `Conflict`는 409, `Unavailable`은 503이다.
* **`Context`:** `config()`, `upstream()`(프록시의 서비스 자격으로 이벤트 서버를 부른다), `authenticate_by_config()`를 준다.
* **`deliver`는 동기 함수다.** 허브의 단일 태스크에서 이벤트 하나당 사용자 수만큼 불린다. 여기서 기다리면 프록시의 모든 사용자가 함께 기다린다. 비동기 판단은 `subscribe`와 `publish`에서 한다. 이 둘은 사용자 자신의 태스크에서 돌므로 기다려도 그 사용자만 늦어진다.
* **범위 검사는 기본 훅의 일이다.** `publish`를 직접 쓰면서 `publish_within_scope`를 부르지 않으면 채널 범위 검사가 빠진다.

## 모듈

| 모듈 | 내용 |
| --- | --- |
| `hooks` | `Hooks`, `Defaults`, `Context`, 기본 규칙 함수 |
| `server` | `Server`, `Bound`, `LaunchError` (feature `ws` 또는 `http`) |
| `protocol` | `router`, `protocol::ws`, `protocol::http` |
| `handshake` | `Handshake`, 쿼리 해석 |
| `config` | JSON 설정. 예시: [proxy.example.json](proxy.example.json) |
| `auth` | `User`, `Denied`, 설정의 인증 모드: `none`, `static`(Q1(c)), `worker`(Q1(a)) |
| `hub` | 상위 연결 하나, 채널마다 상위 구독 하나, 사용자 팬아웃 |
| `message` | 사용자 JSON 형식, `Outcome`, `Rejection`, `Cause` |
| `upstream` | 상위 연결 공급자. `Remote`(이진 프로토콜), `InProcess`(같은 프로세스의 엔진) |

## 설정

| 키 | 뜻 | 기본값 |
| --- | --- | --- |
| `listen` | 프록시가 듣는 주소. 켠 프로토콜이 모두 여기서 열린다 | 필수 |
| `upstream.address`, `upstream.token` | 이벤트 서버 주소와 이 프록시의 서비스 자격 토큰 | 필수 |
| `auth.mode` | `none` / `static` / `worker`. 기본 `authenticate` 훅이 쓴다 | `none` |
| `auth.tokens` | `static`용 `[{token, user, channels}]`. `channels`가 비면 기본 채널 | `[]` |
| `auth.worker` | `worker`용 `{channel, type, reply_channel, timeout_ms}` | — |
| `channels` | 인증이 따로 정하지 않을 때 사용자가 쓸 수 있는 채널. 정확한 이름 또는 `*`로 끝나는 접두사 | `["*"]` |
| `user_buffer` | 사용자 송신 버퍼 메시지 수. 가득 차면 그 사용자를 끊는다(손실 신호) | `1024` |
| `delegate` | 등록에 최종 사용자를 `delegate`로 싣는다. 상위 자격에 위임 권한이 있어야 한다 | `true` |
| `reconnect.initial_ms`, `reconnect.max_ms` | 상위 재접속 백오프의 첫 대기와 상한 | `100`, `5000` |

## 동작

* **구독 집약.** 이벤트 서버 쪽 구독은 채널마다 하나다. 사용자가 가입할 때마다 프록시는 상위에 구독을 한 번 더 보내고(서버는 같은 연결의 중복 구독을 하나로 둔다), 그 응답의 `Subscribed` 표지를 읽은 뒤에 그 사용자를 활성으로 만든다. 표지 이전에 받은 이벤트는 이미 활성이던 사용자에게만 간다. 그래서 늦게 붙은 사용자나 `0→1→0→1` 재구독에 옛 이벤트가 섞이지 않는다. `deliver` 훅이 사용자별로 걸러도 상위 전달은 채널당 한 번이다.
* **마지막 사용자가 떠나면** 상위 구독을 취소한다. 구독과 취소는 연결마다 작업자 하나가 낸 순서대로 보낸다.
* **상위가 끊기면** 백오프로 재접속하고, 사용자가 남아 있는 채널을 모두 다시 구독한다. 끊긴 동안의 발송은 잃는다(Q8(a)).
* **인증 워커(Q1(a)).** 프록시는 `auth.worker.channel`에 착수용 이벤트를 등록한다. payload는 `{"token":…}`, correlation은 무작위 값, `reply`는 `auth.worker.reply_channel`이다. 인증 워커는 그 이벤트를 완료하면서, 같은 correlation으로 reply 채널에 발송용 후속 `{"ok":true|false,"user":"…","channels":["…"]}`를 낸다. `timeout_ms` 안에 답이 없으면 거절한다.
* **HTTP는 요청마다 따로 인증한다.** SSE 유지용 주기 핑은 보내지 않는다(타이머 없음).

## 타이머

이 크레이트가 시계를 기다리는 곳은 둘이다.
* 상위 재접속 백오프(`hub`)
* 인증 워커 답의 기한(`hub`, `Proxy::authenticate_by_config`)

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

`cargo test -p ruvent-proxy`

| 파일 | 판정 |
| --- | --- |
| `tests/hub.rs` | 집약, 늦은 가입, 재구독, 상위 구독 취소, 느린 사용자 차단, 채널 범위, 위임 신원, 정적·워커 인증, 상위 재접속 |
| `tests/ws.rs`, `tests/http.rs` | 실제 TCP로 각 프로토콜의 인증·구독·수신·발행과 거절 |
| `tests/hooks.rs` | `Server`로 만든 프록시. 설정 파일만으로 두 프로토콜이 한 주소에서 열리는지, 쿠키 인증과 사용자 데이터, 구독·등록 훅의 거절과 재작성, 사용자별 `deliver` |

feature 조합(`ws`만, `http`만, 없음)은 `cargo xtask verify`가 각각 린트한다.
