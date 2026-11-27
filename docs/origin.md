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

### 브로커를 여러 대 두는 이유

저장소는 PostgreSQL(PGMQ + `event_store`) 하나다. 브로커 인스턴스를 여러 대 두는 것은 서로를 잇기 위해서가
아니라 **클라이언트 연결과 발송 부하를 나누기 위해서**다 (2026-11-22 사용자 설명).

* 브로커의 주 임무는 자기에게 붙은 클라이언트들이 구독한 채널의 이벤트를 저장소에서 모아, 클라이언트마다
  따로 보내는 것이다.
* 브로커는 클라이언트마다 마지막으로 보낸 이벤트를 안다(`event_subscription_cursor`). 채널에 새 이벤트가
  생기면 그 채널을 받는 클라이언트마다 그 이후를 보낸다. `transport/websocket.ts`는 한 건을 보낼 때마다
  커서를 저장한다.
* 클라이언트는 여러 브로커에 나뉘어 붙는다. 저장소가 하나이므로 어느 브로커에 붙어도 같은 이벤트를 본다.

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

2026-11-23 재구축 이후의 대응이다. 재구축 전의 대응은 git 이력(`061c10a` 이전)에 있다.

| 원본 | ruvent | 달라진 점 |
| --- | --- | --- |
| PGMQ `send` / `read` / `delete` / `set_vt` | 메서드 1(착수용 등록) / 6(구독)+3(착수) / 4(완료) / 착수 기한 | 행 잠금 대신 단일 작성자. visibility 대신 착수 블록의 기한, 회수는 다음 등록 때 |
| `EventStore.append` + 로그 tail + 커서 | 메서드 2(발송용 등록): 그 순간의 구독 접속에 넘기고 끝 | 보존 로그·커서·범위 읽기 없음. 끊긴 접속은 놓친다 |
| `ExecutionStore.claim/complete/release`, 시도 상한, dead letter | 메서드 3·4, 착수(이벤트 id, 워커 uuid), 재시도 카운트. 상한·실패 처리는 워커(핸들러가 `Context::failures()`를 보고 정한다) | 서버에 정책이 없다 |
| 리액션 표(사실 → 큐) | 완료 보고에 실은 후속 이벤트 | 워커가 무엇을 낼지 정한다 |
| `transaction_key` 멱등·충돌 판정 | 큐에 있는 id 중복 거절(G8), 작업 키(G3) | 완료 뒤의 멱등은 워커가 작업 키로 |
| 브로커 여러 대(연결·발송 분산) | 프록시(`ruvent-proxy`로 빌드한 서버): 채널별 상위 구독 하나 | 서버 연결 수 = 프록시 수 + 워커 서버 수 |
| 워커 서버(`createWorkerServer`), 인라인/스레드 풀 | `ruvent-client` `Worker`·`Service`·실행기. 내장도 원격도 같은 메서드 | 허가는 핸들러가 끝나면 반환 |
| heartbeat·이중 리스 | 착수 기한 하나(워커가 기간으로 정함) | 기한 연장 없음. 긴 일은 실행기 이벤트로 |
| `event_store` 영구 이력 | 운영 스냅샷(고른 채널, 별도 파일) | 재생하지 않는다 |
| 게이트웨이 인증 훅 | 서비스 자격(서버), 최종 사용자 인증은 프록시(none / static / worker) | 프록시에 사용자 코드가 없다 |
| 엔벨롭 문자열 필드 18개 | 단일 이진 엔벨롭(고정 머리 27바이트) | 이름은 번호표로, 착수 정보는 작은 숫자 몇 개 |

## 원본에 있고 여기 없는 것

* 영구 이벤트 이력(`event_store` 30일 보존)과 범위 읽기. 운영 스냅샷은 기록일 뿐 재생 경로가 아니다.
* 끊긴 클라이언트의 재접속 재전송(Q8). 프록시의 사용자별 재접속 버퍼도 아직 없다.
* 같은 트랜잭션의 여러 제출자에게 각각 결과를 돌려주는 것.
* PostgreSQL의 복제·시점 복구·운영 도구. 서버는 단일 프로세스·단일 저널이다.
* HTTP 백엔드(`/api/auth/*`, 정적 번들). 프레임워크의 일이 아니다.

## 원본에서 발견한 문서·코드 불일치

* `event-machine.md`는 progress append를 체인으로 직렬화한다고 하지만 코드는 병렬이고,
  순서는 payload의 `seq`로 독자가 복원한다.
* `packages/agent/docs/usage.md`의 `createResultRouter`와 `router` 역할은 코드에 없다.
* `architecture.md`는 남은 큐가 `agent_requests` 하나라고 하지만 실제 배포는 `vibe_*` 여섯 개다.
* `EventStore.append`의 `afterAppend` 훅(큐 전송을 같은 트랜잭션에 넣는 자리)은 현재 호출처가 없다.
