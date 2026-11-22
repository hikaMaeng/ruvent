# 원본 분석과 대응

ruvent는 `F:\dev\ndx_business`의 `packages/agent`(TypeScript, PostgreSQL + PGMQ)의 코어를
다시 쓴 것이다. 이 문서는 원본이 어떻게 동작하는지와, 각 부분이 여기서 무엇이 되었는지를 적는다.

## 원본의 구조

참가자 넷이 PostgreSQL 하나만 공유하고 서로를 호출하지 않는다.

| 참가자 | 가진 것 | 갖지 않은 것 |
| --- | --- | --- |
| 브로커 | 소켓, 인증, 구독 허브, 로그 tail, 리액션 적재 | 핸들러, payload 해석 |
| 워커 서버 | 큐 소비, 클레임, 리스, 종결 기록 | 소켓, 포트 |
| PGMQ | 일감 보관 | 라우팅 |
| 클라이언트 | 송수신, 커서 재개, `eventId` 중복 제거 | 큐 접근 |

* 명령 경로는 큐(정확히 한 워커에게), 관측 경로는 `event_store` 로그(관심 있는 전부에게).
* 브로커가 `action → 큐 목록` 표를 받아 기록된 사실을 큐로 복사한다.

## 무거운 작업을 짧게 끊는 방법 (원본)

| 장치 | 내용 | 원본 위치 |
| --- | --- | --- |
| saga 분해 | 핸들러가 제어구조를 갖지 않는다. 스텝 하나가 트랜잭션 하나 | `apps/vibeagent/docs/event-machine.md` |
| 대기 시간별 큐 분리 | ms짜리(intake·decide·join) / 추론 / bash / 읽기 모델 | `apps/vibeagent/src/server/roles/` |
| 인라인 vs 스레드 | 기다리는 핸들러는 메인 스레드 async, 계산은 worker thread | `broker/worker/inline.ts`, `pool.ts` |
| 공유 in-flight 예산 | 여러 lane이 한 상한을 나눈다 | `broker/loops/worker-consumer/index.ts` |
| 기다리지 않는 emit | `void append`, 순번은 8192개 블록 선예약 | `vibeagent_domain/.../session-context.ts` |
| 별도 OS 프로세스 | bash는 spawn, abort·타임아웃에 SIGKILL | `vibeagent_domain/.../tool/bash.ts` |
| 이중 리스 | visibility 60s < 실행 리스 120s, heartbeat 20s | `worker-consumer`, `idempotency/store.ts` |
| fence | 되돌릴 수 없는 일 직전의 소유 확인 | `broker/worker/entry.ts` |
| 층별 멱등 | 사실=논리키, 델타=펜싱, 팬인=클레임 | `vibeagent_domain/docs/constraints.md` |

## 코드에서 센 비용 (원본)

| 지점 | 비용 |
| --- | --- |
| 명령 1건 | PG 왕복 약 19회: command append 7(BEGIN, advisory lock, 조회, 순번, insert, notify, COMMIT) + claim 2 + complete 1 + recipients 1 + result append 7 + delete 1 |
| emit 1건 | append 트랜잭션 1개(왕복 7) |
| 소켓 전달 | 이벤트 1건마다 커서 `UPDATE` 완료 후 다음 건 (`transport/websocket.ts`) |
| 로그 tail | drain마다 채널 전체 이력 `GROUP BY max` (`channelHighWater`) |
| 리액션 적재 | drain마다 action 전 이력 집계, 커서는 전 스트림 위치를 담은 JSON 한 행 |
| 스트림 순번 | `event_stream_sequence` 행 잠금이 한 세션의 append를 직렬화 |

## 대응표

| 원본 | ruvent | 달라진 점 |
| --- | --- | --- |
| PGMQ `send`/`read`/`delete`/`set_vt`/`archive` | `Engine::submit`/`dequeue`/`ack`·`finish`/`nack`·`release`/(소진 시 자동) | 행 잠금 대신 단일 작성자. 롱폴 대신 엔진 안 주차 |
| `EventStore.append` | `State::append` | 트랜잭션 7왕복 → 메모리 적용. 기본값에서 디스크 기록 없음 |
| `ExecutionStore.claim/complete/release` | `State::begin/finish/release` | command append와 소유 획득이 한 연산, complete·result·delete가 한 연산 |
| `ExecutionStore.renew` + heartbeat 타이머, visibility < lease 이중 시계 | 없음. 스텝 기한 하나 | 같은 프로세스에서는 감독 태스크가 즉시 안다 |
| `agent_execution_recipient` (같은 키의 재제출이 실행에 합류) | 없음. 재제출은 `Duplicate`로 버림 | 이벤트 하나는 워커 하나 |
| `TerminalPersistenceError` 재시도 경로 | 없음 | 종결 기록과 삭제가 원자적이라 그 사이가 없다 |
| `startReactionEnqueue` + 커서 + reconcile sweep + advisory lock 리더 | 리액션 표를 `append` 안에서 적용 | 같은 커밋. 유실 구간이 없어 복구 장치가 없다 |
| `startEventLogTail` + `pg_notify` + 폴링 폴백 | `Subscription` + `Notify` | 프로세스 내 깨우기 |
| `EventStreamHub` + `ConnectionMailbox` + `ReplayBuffer` | `Subscription`(당겨 읽기) | 로그가 버퍼. 메일박스·인계 구간 없음 |
| `event_subscription_cursor` 테이블 | 클라이언트가 든 `position` | 이벤트마다의 커서 `UPDATE` 없음 |
| `(stream_id, sequence)` 위치 맵 | `position` 숫자 하나 | 커밋이 전역 직렬이라 가능 |
| `Sequencer` (8192 블록) | 없음 | emit 순서가 곧 커밋 순서 |
| `createWorkerPool`(thread) / `createInlinePool` | tokio 태스크 / `spawn_blocking` | isolate 비용 없음 |
| `AbortSignal` | future drop + `CancellationToken` | 취소가 협조가 아니라 구조 |
| `deterministicEventId` (SHA-256) | `EventId::derive` (xxh3), 진행 이벤트는 위치에서 생성 | 이벤트당 해시 비용. 식별자는 원본과 일치하지 않는다 |
| 엔벨롭 문자열 필드 (이벤트마다 복사) | `Arc<str>` 공유 | 파생 이벤트가 복사 없이 상속 |
| `prune` 타이머 (1시간마다) | append·완료 시점에 한도 적용 | 청소 주기가 없다 |
| `payload.userId` 덮어쓰기 | 엔벨롭 `principal` | payload를 건드리지 않는다 |
| `createEventBroker` + `createWorkerServer` | `App` + `Service` | 큐·허용 목록·리액션 표가 등록에서 유도된다 |
| WebSocket 고정 | `Transport` 플러그인 | 프로토콜 선택 가능 |

## 원본에 있고 여기 없는 것

* 여러 프로세스·장비로 브로커와 워커를 흩는 확장.
* 영구 이벤트 이력(`event_store` 30일 보존). 여기서 로그는 한정된 버퍼다.
* 같은 트랜잭션의 여러 제출자에게 각각 결과를 돌려주는 것.
* PostgreSQL의 복제·시점 복구·운영 도구.
* 계정 서비스 연동(`createSessionVerifier`). `Authenticator`를 구현해 붙인다.
* HTTP 백엔드(`/api/auth/*`, 정적 번들). 프레임워크의 일이 아니다.

## 원본에서 발견한 문서·코드 불일치

* `event-machine.md`는 progress append를 체인으로 직렬화한다고 하지만 코드는 병렬이고,
  순서는 payload의 `seq`로 독자가 복원한다.
* `packages/agent/docs/usage.md`의 `createResultRouter`와 `router` 역할은 코드에 없다.
* `architecture.md`는 남은 큐가 `agent_requests` 하나라고 하지만 실제 배포는 `vibe_*` 여섯 개다.
* `EventStore.append`의 `afterAppend` 훅(큐 전송을 같은 트랜잭션에 넣는 자리)은 현재 호출처가 없다.
