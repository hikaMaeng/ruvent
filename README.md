# ruvent

Rust로 쓴 이벤트 서버와 그 위의 워커·프록시 프레임워크다. `F:\dev\ndx_business`의 `packages/agent`(TypeScript,
PostgreSQL + PGMQ)를 대체하려고 만들었다. 설계와 그 근거는
[재설계 제안서](docs/proposals/event-server-redesign.md)에 있다.

* **이벤트 서버는 일곱 개의 메서드만 제공한다.** 착수용 등록, 발송용 등록, 착수 마킹, 완료 보고, 삭제, 구독,
  구독 취소다.
* **이벤트는 두 종류다.** 착수용 이벤트는 완료 보고로 큐에서 지워지고, 발송용 이벤트는 발송하는 순간 지워진다.
  둘 다 한 가지 엔벨롭을 쓴다(고정 머리 27바이트).
* **서버는 접속자를 구분하지 않는다.** 워커 서버, 프록시, 내장 워커 모두 같은 메서드를 부른다. 내장 워커는
  메서드를 함수로 부르고, 나머지는 소켓으로 부른다.
* **정책은 워커가 갖는다.** 재시도 상한, 실패 처리, 후속 이벤트는 워커 쪽의 일이다.
* **저널링은 메서드가 아니다.** 서버는 미처리 착수용 이벤트를 저널에 남겨 재기동 때 다시 올린다. 설정이 있으면
  고른 채널을 운영 스냅샷 파일로 모은다.

```text
일반 클라이언트 ──► 프록시(ruvent-proxy-ws / -http, JSON 설정) ──┐
                                                               ├──► 이벤트 서버(ruvent-server: 메서드 7개, 단일 작성자)
                    워커 서버(ruvent-client: App·Service) ─────┘      ├ 착수용 큐: 완료 보고로 제거, 저널 대상
                    내장 워커 = 같은 메서드를 함수로 호출                └ 발송: 등록 순간 구독 접속에 넘기고 제거
```

## 크레이트

| 크레이트 | 내용 | 문서 |
| --- | --- | --- |
| `ruvent` | 엔벨롭과 코덱, 이름표, 엔진(두 큐, 메서드 일곱 개), 저널 v2, 운영 스냅샷, 메서드 계약과 인프로세스 연결 | [crates/ruvent/README.md](crates/ruvent/README.md) |
| `ruvent-protocol` | 이진 와이어 프로토콜: 핸드셰이크, 프레임, 접속별 이름 사전, 메서드 레코드 | [crates/ruvent-protocol/README.md](crates/ruvent-protocol/README.md) |
| `ruvent-server` | 이벤트 서버 바이너리와 수신부. JSON 설정 | [crates/ruvent-server/README.md](crates/ruvent-server/README.md) |
| `ruvent-client` | 원격 연결과 워커 프레임워크(`App`, `Service`, 실행기, 재시도 정책) | [crates/ruvent-client/README.md](crates/ruvent-client/README.md) |
| `ruvent-proxy` | 프록시 공통: JSON 설정, 최종 사용자 인증, 채널별 상위 구독 하나 | [crates/ruvent-proxy/README.md](crates/ruvent-proxy/README.md) |
| `ruvent-proxy-ws` | WebSocket 프록시 바이너리 | [crates/ruvent-proxy-ws/README.md](crates/ruvent-proxy-ws/README.md) |
| `ruvent-proxy-http` | HTTP 프록시 바이너리(POST 등록, SSE 구독) | [crates/ruvent-proxy-http/README.md](crates/ruvent-proxy-http/README.md) |
| `xtask` | 검증 관문 `cargo xtask verify` | [docs/testing.md](docs/testing.md) |

## 문서

| 문서 | 내용 |
| --- | --- |
| [docs/overview.md](docs/overview.md) | 무엇을 하고 무엇을 하지 않는가, 보장 계약 |
| [docs/architecture.md](docs/architecture.md) | 엔진 구조, 엔벨롭, 저널, 와이어, 시간과 순서 |
| [docs/usage.md](docs/usage.md) | 내장 엔진, 서버, 워커, 프록시를 쓰는 법 |
| [docs/constraints.md](docs/constraints.md) | 결정된 제약과 그 대가 |
| [docs/testing.md](docs/testing.md) | 관문, 테스트 수준, 요구사항 ↔ 테스트 지도 |
| [docs/origin.md](docs/origin.md) | 원본(`packages/agent`) 분석과 대응 |
| [docs/proposals/event-server-redesign.md](docs/proposals/event-server-redesign.md) | 재설계 제안서(승인됨)와 구현 기록 |

## 검증

```sh
cargo xtask verify
```

마지막 실행 기록은 [docs/reports/test-2026-11-23.txt](docs/reports/test-2026-11-23.txt), 측정은
[docs/reports/bench-2026-11-23.txt](docs/reports/bench-2026-11-23.txt)에 있다.
