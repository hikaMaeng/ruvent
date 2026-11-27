# ruvent

Rust로 쓴 이벤트 서버 라이브러리다. 서버 한 대가 이벤트 장부를 메모리에 두고 자체 저널 파일로 재시작 후 복구한다.
그 위에 워커 SDK와 프록시 골격이 있다. 처음에는 [ndx_business](https://github.com/hikaMaeng/ndx_business)의
`packages/agent`(TypeScript, PostgreSQL + PGMQ)를 대체하려고 만들었다.

## 하는 일

이벤트는 두 종류다.

| | 착수용 이벤트 | 발송 이벤트 |
| --- | --- | --- |
| 받는 쪽 | 참여한 워커 중 하나 | 등록 시점의 채널 구독자 전부 |
| 지우는 때 | 그 워커의 완료 보고 | 구독자에게 넘긴 직후 |
| 저장 | 저널에 기록, 재시작 후 복구 | 저장 안 함 |
| 실행 횟수 | 최소 1회 | 등록 시점 구독자에게 1회 |
| 쓰임 | API 중계, 계산, 다음 단계 작업 | 결과, 스트리밍 조각, 알림 |

채널은 이름 없는 UUIDv7이고 보통 요청 하나에 채널 하나를 쓴다. 타입(u16)은 서버가 착수용 이벤트를 워커에게 나눌 때만 쓴다.
엔벨롭은 바이너리이고 고정 머리가 38바이트(id, 채널, 타입, flags, 실패 횟수)다. payload는 서버가 읽지 않는다.

착수는 (이벤트 id, 워커 uuid) 한 쌍으로 식별한다. 워커는 착수할 때 lease(착수 유지 기한)를 정한다. 기한 안에 완료나 연장,
반환 중 하나를 보낸다. 기한이 지나면 서버가 이벤트를 회수해 실패 횟수를 1 올리고 다른 워커에게 넘긴다.

서버에 넣지 않은 것도 정해 두었다. 재시도 상한, 만료, 순서, 흐름 제어, 채널별 권한은 워커와 프록시가 맡는다. 서버는 실패 횟수만 센다.

```text
클라이언트 ──► 프록시(프로젝트가 만든다. 골격은 ruvent-proxy) ─┐
                                                              ├──► ruvent-server (wire v6, TCP)
              워커 서버(ruvent-client: Worker · Service) ──────┘     착수용 큐: 완료 보고 시 삭제, 저널에 기록
              내장 워커: 같은 메서드를 소켓 없이 함수로 호출            발송: 등록 즉시 구독자에게 넘기고 삭제
```

일반 클라이언트는 이벤트 서버에 직접 붙지 않는다. HTTP 같은 클라이언트 프로토콜, 인증, 요청-응답 매칭, 역압은 프록시가 처리한다.

## 응용 사례

ruvent로 만든 서비스가 둘 있다. 둘 다 이 저장소 옆에 나란히 받으면 그대로 빌드된다(아래 "받아서 빌드하기").

### apibondrust (LLM API 중계기)

[apibondrust](https://github.com/hikaMaeng/apibondrust)는 OpenAI Chat, Anthropic Messages, Responses, Embeddings 요청을
여러 추론 서버로 나눠 보내는 중계기다. 한 프로세스로 된 원본 [apibond](https://github.com/hikaMaeng/apibond)를 ruvent 위에서
게이트웨이와 워커로 나눠 다시 만들었다.

| 프로그램 | ruvent에서 맡는 역할 |
| --- | --- |
| `apibond-gateway` | 프록시. HTTP 요청마다 새 채널을 만들어 구독한 뒤 그 채널에 요청을 착수용 이벤트로 등록 |
| `apibond-worker` | 타입으로 참여한 워커. 착수할 때 추론 서버를 골라 중계하고, 스트리밍 조각과 결과를 같은 채널로 발송 |
| `ruvent-server` | 이벤트 서버. 8 MiB를 넘는 요청 본문은 4 MiB 조각으로 나뉘어 저널에 기록 |

ruvent 5.1 기준으로 시험 68개가 통과했고 실패는 0개다(`tests/reports/unit/20261126_220119.txt`). 실제 프로세스를 띄우는
인수 시험은 최종 7/7이다(`tests/reports/acceptance/20261126_223019`). 같은 판본의 첫 두 인수 실행은 2/7이었고 원인을 아직 찾지
못했다. 원본과 비교한 측정(2026-11-25, `tests/reports/performance/20261125_220425-compare.md`)에서는 1 KiB 요청, 동시 64일 때
p50이 1.311 → 1.857 ms, 처리량이 25572 → 20472 req/s였다. 16 MiB 본문은 이벤트 서버를 한 번 더 거치는 비용이 커서
처리량이 57.7 → 13.3 req/s로 떨어졌다.

### ruvent-chat (터미널 채팅)

[ruvent-chat](https://github.com/hikaMaeng/ruvent-chat)은 터미널(TUI)에서 쓰는 채팅 서비스다. 채팅 서버에 붙으면 방 목록이
보이고, 방을 열거나 들어가거나 나갈 수 있다. 서버는 클라이언트를 uuid로만 구분하고, 닉네임은 클라이언트가 정해 payload에 실어 보낸다.

| 프로그램 | ruvent에서 맡는 역할 |
| --- | --- |
| `chat-proxy` | 프록시. TUI가 TCP로 붙는 채팅 서버. 방 명령은 착수용 이벤트, 메시지는 발송 이벤트로 등록 |
| `chat-worker` | 워커 서버. 방 목록과 참가자를 관리하고, 서비스 전체에 하나인 생존 확인 핑의 라운드를 정함 |
| `chat` | TUI 클라이언트. 소켓 하나로 보내고 받기를 동시에 처리 |

방을 연 사람은 바로 그 방에 들어가고, 한 클라이언트는 한 번에 한 방에만 있다. 방에 아무도 남지 않으면 그 자리에서 지운다.
명시적으로 나간 경우와 핑에 응답하지 않아 자동으로 빠진 경우가 모두 해당한다.

시험 82개가 통과했고 실패는 0개다(측정 시험 2개는 기본 실행에서 제외, `docs/reports/test-2026-11-27-split.txt`). 세 프로그램을
따로 띄운 측정(참가자 20명)에서 메시지가 모든 참가자에게 닿는 지연은 p50 406 µs, p99 520 µs였다
(`docs/reports/measure-2026-11-27-split.txt`).

## 받아서 빌드하기

Rust 1.89 이상이 필요하다. 두 응용 저장소는 ruvent를 `../ruvent` 상대 경로로 참조하므로 같은 폴더 아래에 나란히 받는다.

```sh
git clone https://github.com/hikaMaeng/ruvent
git clone https://github.com/hikaMaeng/apibondrust
git clone https://github.com/hikaMaeng/ruvent-chat
cd ruvent
cargo xtask verify
cargo run --release -p ruvent-server -- crates/ruvent-server/server.example.json
```

설정은 JSON 파일 하나다. 필수 항목은 `listen`과 `credentials`이고 나머지는 기본값이 있다.
전체 키는 [crates/ruvent-server/README.md](crates/ruvent-server/README.md)에 있다.

## 크레이트

| 크레이트 | 내용 | 문서 |
| --- | --- | --- |
| `ruvent` | 엔벨롭과 코덱, 채널(UUIDv7), 엔진, 저널 형식 7, 운영 스냅샷, 메서드 정의와 프로세스 내부 연결 | [crates/ruvent/README.md](crates/ruvent/README.md) |
| `ruvent-protocol` | 바이너리 wire v6: 핸드셰이크, 호출 · 응답 · 알림 · 전달 레코드(이름 사전 없음) | [crates/ruvent-protocol/README.md](crates/ruvent-protocol/README.md) |
| `ruvent-server` | 이벤트 서버 실행 파일과 수신부. JSON 설정 | [crates/ruvent-server/README.md](crates/ruvent-server/README.md) |
| `ruvent-client` | 원격 연결 SDK와 워커 프레임워크(`Worker`, `Service`, 실행기, 재시도) | [crates/ruvent-client/README.md](crates/ruvent-client/README.md) |
| `ruvent-proxy` | 프록시 골격: 서버별 관찰 연결, 채널마다 상위 구독 하나로 나눠 주는 팬아웃. 클라이언트 쪽 프로토콜 · 인증 · 요청-응답 · 기한 · 역압은 프로젝트 몫 | [crates/ruvent-proxy/README.md](crates/ruvent-proxy/README.md) |
| `xtask` | 검증 명령 `cargo xtask verify` | [docs/testing.md](docs/testing.md) |

메서드는 권한 단위로 일곱 개다. 착수용 등록, 발송 등록, 착수(`claim`: `Join` · `Claim`), 완료(`complete`: `Complete` ·
`Extend` · `Release`), 삭제, 구독, 구독 취소. 운영용 `operate`(`Stats` · `Drain`)는 따로 있다.

## 문서

| 문서 | 내용 |
| --- | --- |
| [docs/overview.md](docs/overview.md) | 하는 것과 하지 않는 것, 보장 계약 |
| [docs/architecture.md](docs/architecture.md) | 엔진 구조, 엔벨롭, 저널, 와이어, 시간과 순서 |
| [docs/usage.md](docs/usage.md) | 내장 엔진, 서버, 워커, 프록시를 쓰는 법 |
| [docs/constraints.md](docs/constraints.md) | 정한 제약과 그 대가 |
| [docs/testing.md](docs/testing.md) | 검사 단계, 시험 수준, 요구사항 ↔ 시험 지도 |
| [docs/origin.md](docs/origin.md) | 원본(`packages/agent`) 분석과 대응 |
| [docs/proposals/](docs/proposals/) | 재설계 제안서와 개편 계획, 결정 기록 |
| [docs/intro/](docs/intro/README.md) | 처음 보는 개발자용 소개 덱(18장)과 생성기 |

## 검증

```sh
cargo xtask verify
```

검사 11단계(서식, 린트, API 문서, 정적, 상태 기계, 런타임, 인수, 프로토콜, 서버, 워커, 프록시)를 차례로 돌린다. 마지막 실행 기록은
[docs/reports/test-2026-11-26-worker-uuid-claim.txt](docs/reports/test-2026-11-26-worker-uuid-claim.txt)이고 시험 264개가 통과,
0개가 실패했다. 측정 기록은 [docs/reports/bench-2026-11-23.txt](docs/reports/bench-2026-11-23.txt)에 있다(판본 5.1 이전 엔진).
