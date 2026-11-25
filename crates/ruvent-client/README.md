# ruvent-client

이벤트 서버의 클라이언트 SDK(계획 2.9). 서버가 부하를 클라이언트로 넘기는 설계이므로, 클라이언트가 지켜야 할
규칙(재접속, 미확인 버퍼, 서버 선택, 정지 통보, 워커 창, lease, 종료, 요청–응답)을 여기서 구현한다.

| 모듈 | 하는 일 |
| --- | --- |
| `servers` | 서버 목록 공급자 `ServerList`(현재 목록 + 변경 알림). 정적·교체 가능한 구현 `Servers` |
| `link` | 연결 하나를 여는 `Connector`. `Tcp`(wire v2), `Local`(프로세스 안 엔진, 이름 → `Engine`, 재기동 때 교체) |
| `line` | 서버 하나 × 역할 하나: 지수 백오프 + full jitter(S20) 재접속, `Bye` 사유표, `Welcome`의 server_id·세대 변화(재기동) 감지 |
| `buffer` | 번호 붙은 연결의 미확인 버퍼: 누적 `Committed`로 비우기, `resume` 재개, 기록을 잃은 서버의 다시 번호 매기기 |
| `producer` | `Producer`: 선택기(`Selector`, 기본 `LeastUnacked`), 바이트 상한(S8, 가득이면 `send`가 기다림), `Paused`/`Resumed`, `draining`, 재시도 예산(S9) |
| `worker` | `Worker`: 서비스별 슬롯 P, 서버 사이 창 규칙(`worker::windows::Book`), 자동 연장, 실행 한도, 핸들러의 세 끝(`Retry`), 실행 중 방출, 종료 절차 |
| `requester` | `Requester`: 응답 채널 `reply.<인스턴스 id>`, 서버마다 한 번 참여(발송 포함, lease 5초), 대기자 표, 조각 유실 표시, 같은 id 재전송(`Stream::resend`). 기한을 두지 않고 조각을 버리지 않는다(`Stream::backlog`) |
| `observer` | `Observer`: 서버마다 관찰 연결, 연결마다 구독 재발행, 원 엔벨롭 수신, `Stats`·`Drain` |
| `remote` | 전송층: `connect`/`open`(코어 `Client`), `open_raw`(SDK가 쓰는 날 연결). 모든 소켓에 keepalive(S13) |

## 시작

```rust,ignore
let tcp = Arc::new(ruvent_client::link::Tcp::new(token));
let servers = ruvent_client::servers::Servers::new(["10.0.0.1:7400", "10.0.0.2:7400"]);
let producer = Producer::start(tcp.clone(), &servers, ProducerConfig::default());
let worker = Worker::builder(tcp.clone(), &servers).service(Service::new("orders").on("create", handler)).start();
let requester = Requester::start(tcp.clone(), &servers, RequesterConfig::default());
let (observer, stream) = Observer::start(tcp, &servers, ObserverConfig::default());
```

같은 코드를 `link::Local`로 바꾸면 프로세스 안 엔진에 붙는다(시험과 내장 사용).

## 설정값(계획 8장, 모두 필드와 기본값)

| 값 | 기본 | 위치 |
| --- | --- | --- |
| 재접속 백오프 초기·상한(S20, `draining` 뒤에도 같다) | 50 ms · 1초 | `line::backoff::Reconnect` |
| 미확인 버퍼 상한(S8) | 연결당 64 MiB | `ProducerConfig::unacked_bytes`, `WorkerConfig::unacked_bytes`, `RequesterConfig::unacked_bytes` |
| 재시도 예산(S9) | 정상 요청의 10% | `ProducerConfig::retry_ratio` |
| lease · 실행 한도(B7) | 30초 · 30초(실행기 1시간) | `Service::lease`, `Service::run_limit`, `Service::executor` |
| 결과 보관 기한(B10) | 60초 | `WorkerConfig::result_keep` |
| 응답 채널 lease(S16) | 5초 | `RequesterConfig::lease` |
| 종료 유예 | 30초 | `WorkerConfig::shutdown_grace` |
| keepalive(S13) | 15초 · 5초 · 3회, `TCP_USER_TIMEOUT` 30초 | `remote::Keepalive` |

## 타이머

네 곳뿐이고 모두 일회성이다(가장 이른 기한에만 깬다): 재접속 백오프(`line`), 자동 연장(`worker/extend.rs`),
종료 유예(`worker/shutdown.rs`), 실행 한도(`worker/run.rs`). 요청자는 기한이 없다. 요청의 기다림은 호출자가 끊는다. 정적 시험
`nothing_polls_and_every_timer_is_declared`가 지킨다.
