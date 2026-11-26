# ruvent-server

이벤트 서버 바이너리와 라이브러리. 엔진 하나 앞에 wire v5(`ruvent-protocol`) TCP 수신부를 둔다.
모든 접속은 `Hello`에서 서비스 자격 토큰과 역할(생산·작업·관찰, 계획 2.2)을 밝히고, 그 뒤로는 그 역할의
호출만 그 자격으로 보낸다. 자격이 할 수 있는 일은 엔진의 권한표(`grants`)가 정한다.

## 실행

```sh
cargo run -p ruvent-server --release -- crates/ruvent-server/server.example.json
```

* Ctrl-C → 정상 종료(아래 "내리는 순서") → 종료 코드 0.
* 설정 오류 → 종료 코드 2.
* 엔진이 스스로 멈춤(저널 쓰기 실패) 또는 수신부 실패 → 원인을 stderr에 쓰고 종료 코드 1.

## 설정 (JSON)

[server.example.json](server.example.json). `listen`과 `credentials`만 필수이고, 나머지는 모두 엔진·서버의
기본값(계획 8장)을 쓴다. 모르는 키는 거절한다.

| 키 | 기본값 | 뜻 |
| --- | --- | --- |
| `listen` | (필수) | `host:port` |
| `journal` | 없음 = 휘발 | 세그먼트 로그 디렉터리. 있으면 착수용 이벤트는 모두 저널에 쓴다. 형식 5 이하와 단일 파일 저널은 거부하고 그대로 둔다(B12) |
| `durability` | `"scheduled"` | `"strict"`: 착수용 이벤트는 등록이 디스크에 닿기 전에 다른 연결에 보이지 않는다(2.7) |
| `server_id` | 기동마다 새로 | `Welcome`의 서버 id(UUID 문자열) |
| `credentials` | (필수, 1개 이상) | `name`, `token`, `methods`(생략 = 여덟 개 전부). 그보다 좁은 범위는 없다 |
| `engine` | 엔진 기본값 | `batch_max`, `inbox`, `outbound`, `backlog_page`, `ack_every`(S2, 256), `reconnect_grace_ms`(S20, 3000), `max_follow_ups`(S10, 1024), `stats_top`(B24, 16) |
| `producers` | B4·S15 | `linger_ms`(1시간), `linger_max_ms`(7일), `max_entries`(100만), `entry_bytes`(64) |
| `memory` | S5·S22 | `limit`(바이트, 생략 = 물리 메모리의 절반), `reserve`(0.10), `high_watermark`(0.80, 넘으면 캐시된 본문을 내린다), `release`(0.875), `slot_bytes`(180), `depth_target`(1억, 보고만 한다) |
| `storage` | S3·S4·S6 | `segment_bytes`(64 MiB), `relocate_at`(0.10), `checkpoint_slice`(4096), `checkpoint_tail_multiple`(8), `checkpoint_tail_cap`(8 GiB), `disk_reserve_segments`(4), `disk_reserve`(0.10), `disk_capacity`(없음), `read_ahead`(16), `max_reads`(64), `checkpoint_queue`(2) |
| `connection` | 서버 기본값 | `max_body`(B13, 16 MiB), `inline`(S11, 256), `direct`(S11, 4096), `handshake_ms`(10000), `unsent`(S7, 256 MiB), `close_ms`(S20, 5000), `spool_piece`(256 KiB), `batch`(1024), `keepalive` |
| `connection.keepalive` | S13 | `idle_ms`(15000), `interval_ms`(5000), `retries`(3), `user_timeout_ms`(30000, 리눅스만) |
| `snapshot` | 없음 | `{ "directory", "types", "max_events", "max_age_ms", "buffer" }` 운영 스냅샷. `types`는 타입 번호 목록(빈 목록·생략 = 모든 타입) |

메서드 이름: `register_claimable`, `register_dispatch`, `claim`, `complete`, `delete`, `subscribe`, `unsubscribe`,
`operate`(`Stats`, `Drain`). 권한은 메서드 단위뿐이다(계획 9.15): 채널은 서버가 아무것도 읽지 않는 UUIDv7이고 아는 쪽이 부른다.
범위를 나눠야 하면 서버를 나눈다. 격리·만료·직렬화·휘발·대기 상한 설정은 없다: 그 정책은 워커와 프록시가 갖는다.

토큰은 설정 파일에 평문으로 있다. 파일 권한으로 보호하고 저장소에 커밋하지 않는다.

## 연결

| 역할 | 읽기 | 쓰기 |
| --- | --- | --- |
| 생산 | 멈추지 않는다. 서버는 생산자를 정지시키지 않는다(계획 9.15) | 누적 `Committed`는 대기열에 하나만 둔다(새 것이 아직 쓰지 않은 것을 덮는다) |
| 작업 | 멈추지 않는다. 메모리가 고수위면 후속 본문을 스풀로 디스크에 흘려 읽는다(2.6 4·5번) | `Assign`은 쓰기를 시작할 때 남은 lease를 싣는다. 그때 이미 lease가 지났으면 보내지 않는다(명시 착수의 답은 `Refused{missing}`). 발송·통지 바이트는 `unsent`(S7)로 묶는다 |
| 관찰 | 들어오는 것은 작다 | 미송신 바이트가 `unsent`(S7)를 넘으면 `Bye{slow}` |

* 수신: 레코드 머리를 먼저 읽고, 본문은 그 이벤트의 버퍼로 바로 받는다(`direct` 이상은 소켓에서 직접).
  프레임 전체를 먼저 할당하지 않는다.
* 송신: 연결별 머리와 공유 본문을 벡터 쓰기로 보낸다. `inline` 미만 본문만 머리 버퍼에 복사한다.
* `Fault`는 (연결, 종류)마다 처음 한 번만 보낸다. 읽을 수 없는 레코드는 `Fault{malformed}` + `Bye{protocol}`.
* 모든 `Bye` 뒤에는 단계로 닫는다: 남은 것을 쓰고 → 쓰기 쪽만 닫고 → 상대가 닫을 때까지 읽어 버리고 → 닫는다.
  상대가 닫지 않으면 `close_ms`에 닫는다. 연결이 닫힐 때 쓰기를 끝내지 못한 `Assign`의 표지를 엔진에 넘겨
  곧바로 대기로 되돌린다(실패 횟수 그대로).
* 모든 소켓에 TCP keepalive를 건다. 리눅스는 `TCP_USER_TIMEOUT`도 건다. 앱 타이머는 없다.
* v1 클라이언트의 hello는 첫 바이트로 알아보고 v1 형식의 `Bye`로 거절한다. v5가 아닌 다른 버전(v2~v4 포함)은 `Bye{version}`.

## 내리는 순서 (`serve`, 계획 2.8, B22)

1. 수신을 멈춘다.
2. `Engine::drain`: 생산 연결마다 최종 `Committed` → `Bye{draining}` → 단계로 닫기. 새 생산 연결은 거절된다.
3. 작업·관찰 연결에 `Bye{draining}`. 쓰기를 시작하지 않은 `Assign`은 대기로 돌아간다.
4. 모든 연결이 엔진에 닫힘을 넘기면 곧바로 `Engine::shutdown`(멈춘 장부의 체크포인트). 다 쓴 `Assign`은
   착수 상태로 재기동을 넘는다. 늦게 닫는 피어가 그사이 재접속 유예를 넘겨 착수를 회수시키지 못한다.
5. 남은 단계로 닫기가 끝날 때까지(각각 `close_ms` 이내) 기다린다.

운영자가 미리 비우려면 `operate` 권한의 관찰 연결로 `Drain`을 보낸다. `Stats`의 대기·착수가 0이면 잃는 것 없이
내릴 수 있다.

## 구조

| 경로 | 내용 |
| --- | --- |
| `src/config/` | JSON 설정 → `EngineConfig`·권한표·자격 목록·`ServerOptions` |
| `src/connection/mod.rs` | `serve`(수신과 내리는 순서), 접속 하나의 조립, `ServerOptions` |
| `src/connection/handshake.rs` | 첫 레코드 `Hello`, v1 알아보기, 토큰 → 자격 |
| `src/connection/read.rs` | 읽기 태스크: 머리 → 본문(메모리 또는 스풀) → `Session::call` |
| `src/connection/write.rs` | 소유자: 엔진의 `Outbound` → 미송신 대기열 → 벡터 쓰기, 연결 끝의 처리 |
| `src/connection/unsent/` | 미송신 대기열(순수 상태): 누적 확인 접기, `Fault` 첫 1회, 바이트 상한 |
| `src/connection/close.rs` | 단계로 닫기와 닫기 기한, 핸드셰이크에서의 거절 |
| `src/connection/socket.rs` | keepalive(`socket2`) |
| `src/lib.rs` | `run`: 세그먼트 로그 복구, 엔진 시작, 수신, 종료·장애 전파 |
| `src/main.rs` | 바이너리 |
| `tests/remote.rs` | 실제 TCP 위의 서버. 클라이언트와 원시 wire v5 연결 |

타이머는 둘이다: hello 기한(`handshake.rs`)과 닫기 기한(`close.rs`). 정적 시험의 `TIMERS`에 선언돼 있다.

## 테스트

```sh
cargo test -p ruvent-server
```

시험 지도는 [docs/testing.md](../../docs/testing.md)의 "서버" 절이다.
