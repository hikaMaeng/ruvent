# ruvent-proxy

프록시의 골조다. 이벤트 서버의 발송 팬아웃 계층(개편 계획 4장)에서, 모든 프록시가 필요로 하면서 틀리기 쉬운 두 가지만 준다.

* 서버 목록의 모든 서버에 붙는 서버별 상향 관찰 연결
* 구독 규칙: 채널마다 서버별 상위 구독 하나, 구성원별 효력 시점

## 범위

이벤트 서버는 프로토콜 하나만 말한다. TCP 위의 ruvent wire(v5)다. 프록시와 워커 서버는 SDK(`ruvent-client`)로 그 프로토콜만 써서 붙는다. 클라이언트는 이벤트 서버에 직접 붙지 않는다.

그 밖은 전부 프록시 프로그램의 것이다. 이 크레이트는 어느 것도 정하지 않는다.

| 프로그램이 정하는 것 | 쓰는 것 |
| --- | --- |
| 클라이언트 쪽 프로토콜(WebSocket, HTTP, WebRTC, …) | 프로그램 자신의 서버 |
| 클라이언트가 누구이고 무엇을 구독·등록할 수 있는지 | 프로그램의 검사 → [`Member::subscribe`] |
| 등록 | 프로그램 자신의 `ruvent_client::producer::Producer` |
| 요청–응답 | 요청 하나가 구성원 하나다. 요청마다 채널(`Channel::new()`, UUIDv7)을 [`Member::subscribe`]로 구독하고, 그 채널에 요청을 프로그램 자신의 `Producer`로 등록하고(타입은 프로그램이 정한 요청 타입), 오는 것을 클라이언트에 쓴다. 요청을 포기했다고 알리는 것도 워커가 낸 이벤트다(서버는 격리하지 않는다). 끝 조건(종결 이벤트, 시간)을 보고 구성원을 버리면 구독이 풀린다. 기한·재전송도 프로그램이 정한다 |
| 느린 클라이언트에 대한 역압 | 골조는 `member_buffer`까지 찬 구성원을 떼어 낸다(스트림이 끝난다). 그것을 클라이언트에게 어떻게 알릴지는 프로그램이 정한다 |
| 준비 상태를 어떻게 내보일지(`/health`, 지표) | 프로그램. 이 크레이트는 [`Health`] 값만 준다 |

## 사용

```rust
use ruvent_proxy::{Channel, Fanout, FanoutConfig, Servers, Tcp, Upstream};
use std::sync::Arc;

let upstream = Upstream::new(Arc::new(Tcp::new("proxy-token")), Servers::new(["10.0.0.1:7400", "10.0.0.2:7400"]));
let fanout = Fanout::start(FanoutConfig::default(), upstream);
// 프로그램 자신의 클라이언트 연결마다:
let (member, mut events) = fanout.attach().await?;
let channel = Channel::new(); // 요청 하나의 스트림, 또는 클라이언트가 받은 채널
member.subscribe(channel).await?; // 프로그램의 검사 뒤에
while let Some(event) = events.recv().await {
    // `event`(Arc<Envelope>)를 프로그램의 프로토콜로 클라이언트에게 쓴다.
}
```

| API | 하는 일 |
| --- | --- |
| `Fanout::start(config, upstream)` | 허브 태스크를 띄우고 목록의 모든 서버에 관찰 연결을 연다. 곧바로 돌아온다 |
| `Fanout::attach()` | 구성원 하나. 스트림은 서버가 보낸 엔벨롭을 `Arc`로 나눠 준다. payload는 건드리지 않는다 |
| `Member::subscribe(channel)` | 자기 구독이 한 서버에서 효력을 가질 때 돌아온다. 같은 구성원의 재호출은 즉시 성공한다 |
| `Member::unsubscribe(channel)` | 마지막 구성원이 떠나면 상위 구독을 한 번 취소한다 |
| `Member` 버림 | 구성원을 떼어 내고 모든 채널에서 뺀다 |
| `Fanout::ready()`, `is_ready()`, `health()`, `watch_health()` | 서버별 관찰 연결과 준비 상태 |
| `Fanout::stop()` | 모든 구성원의 스트림을 끝내고 상향을 놓는다. 모든 핸들이 사라져도 멈춘다 |

`SubscribeError`는 셋이다. `Refused(Refusal)`는 이벤트 서버의 거절이고, `Left`는 효력 전에 떠난 경우, `Stopped`는 팬아웃이 멈춘 경우다.

## 계약

* **효력 시점.** 구성원은 자기 가입이 낸 subscribe의 그 서버 확인을 허브가 읽은 뒤에야 그 서버에서 활성이 된다. 그 앞에 스트림에 있던 것은 이미 활성인 구성원에게만 간다. 그래서 `0 → 1 → 0 → 1` 뒤에도 첫 구독의 것이 새지 않는다.
* **서버별 상향.** 서버 목록 공급자를 따라 모든 서버에 붙는다. 목록에 더한 서버에는 이미 있는 구독을 다시 말한다. 재접속하면 그 서버의 효력이 확인 전까지 비워진다.
* **발송은 1회성.** 끊긴 사이의 발송은 잃는다(0.3).
* **느린 구성원.** 허브는 한 구성원을 기다리지 않는다. 스트림이 `member_buffer`까지 차면 그 구성원을 떼어 내고 스트림을 끝낸다. 나머지는 계속 받는다.
* **준비 상태.** 목록에 서버가 하나 이상 있고 모든 서버의 관찰 연결이 살아 있을 때다. FIN 없는 끊김은 SDK가 모든 소켓에 거는 커널 keepalive(S13)가 찾는다.

## 설정 (`FanoutConfig`, 8장 기본값)

| 필드 | 뜻 | 기본값 |
| --- | --- | --- |
| `reconnect` | 관찰 연결의 재접속 백오프(S20): 첫 상한, 상한 | 50 ms, 1초 |
| `member_buffer` | 구성원 스트림에 쌓아 두는 이벤트 수. 넘으면 떼어 낸다 | 1024 |
| `commands` | 허브 명령 대기열. 차면 호출자가 기다린다 | 4096 |

## 구조

| 폴더 | 내용 |
| --- | --- |
| `fanout` | `mod.rs`: `Fanout`, `Member`, `FanoutConfig`, `SubscribeError`. `channels/`: 구독 장부(순수 상태, 태스크·시계 없음). `task.rs`: 허브 태스크(장부의 단계 실행, 팬아웃, 상태 보고) |
| `health` | `Health`, `ServerHealth`: 서버별 관찰 연결과 준비 상태(순수) |
| `upstream` | `Upstream`: 커넥터, 서버 목록, 자격 |

이 크레이트는 시계를 기다리지 않는다. 재접속 백오프는 SDK의 `line`이 맡는다(`crates/ruvent/tests/static/architecture.rs`의 `TIMERS`). 골조가 정책을 다시 갖지 않도록 `the_proxy_is_a_skeleton`이 막는다. 구독 장부는 순수 상태이고, HTTP·직렬화·생산·요청·훅이 없고, 의존은 `ruvent`, `ruvent-client`, `tokio`뿐이다.

## 테스트

| 파일 | 범위 |
| --- | --- |
| `src/**/tests.rs` | 구독 장부, 준비 상태 계산 |
| `tests/fanout.rs` | 같은 프로세스의 엔진(SDK `Local`): 집약, 효력 시점, 재접속과 재구독, 목록에 더한 서버, 느린 구성원, 원 엔벨롭 공유, 정지와 상향 해제 |
| `tests/acceptance.rs` | 실제 `ruvent-server`에 TCP로 붙인 인수: 효력 시점 불변식, UUIDv7이 아닌 채널의 거절, 서버 2대, 상향 상실과 준비 상태, 요청별 채널(타입 참여 워커, 요청 2건 동시) |

[`Member::subscribe`]: src/fanout/mod.rs
[`Health`]: src/health/mod.rs
