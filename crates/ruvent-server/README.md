# ruvent-server

이벤트 서버 바이너리와 라이브러리. 엔진 하나 앞에 이진 프로토콜(`ruvent-protocol`) TCP 수신부를 둔다.
서버는 워커·프록시·클라이언트를 구분하지 않는다. 모든 접속은 첫 프레임에서 서비스 자격 토큰으로
인증하고, 그 뒤로는 그 자격으로 일곱 메서드를 부른다. 자격이 할 수 있는 일은 엔진의 권한표(`grants`)가 정한다.

## 실행

```sh
cargo run -p ruvent-server --release -- crates/ruvent-server/server.example.json
```

* Ctrl-C → 엔진 정상 종료(남은 저널 기록) → 종료 코드 0.
* 설정 오류 → 종료 코드 2.
* 엔진이 스스로 멈춤(저널 쓰기 실패) 또는 수신부 실패 → 원인을 stderr에 쓰고 종료 코드 1.

## 설정 (JSON)

[server.example.json](server.example.json):

```json
{
  "listen": "127.0.0.1:7400",
  "journal": "data/ruvent.journal",
  "durability": "strict",
  "credentials": [
    { "name": "workers", "token": "…", "methods": ["register_claimable", "claim", "complete", "subscribe"], "channels": ["orders.*"] },
    { "name": "proxy", "token": "…", "channels": ["chat.*"], "delegate": true }
  ]
}
```

| 키 | 기본값 | 뜻 |
| --- | --- | --- |
| `listen` | (필수) | `host:port` |
| `journal` | 없음 = 휘발 | 저널 파일. 상위 디렉터리는 만든다 |
| `durability` | `"scheduled"` | `"strict"`: 착수용 등록·완료·삭제의 응답과 통지를 디스크 기록 뒤에 낸다 |
| `credentials` | (필수, 1개 이상) | `name`, `token`, `methods`(생략 = 일곱 개 전부), `channels`(패턴, 생략 = `["*"]`), `delegate`(위임 신원 허용) |
| `batch_max`, `inbox`, `outbound`, `backlog_page` | 엔진 기본값 | `outbound`는 접속별 송신 버퍼(메시지 수). 가득 차면 그 접속을 끊는다 |
| `compaction` | `{50000, 4}` | `{ "min_records", "ratio" }` |
| `snapshot` | 없음 | `{ "directory", "channels", "types", "max_events", "max_age_ms", "buffer" }` 운영 스냅샷 |
| `dictionary` | 4096 | 접속·방향별 이름 사전 상한 |
| `max_frame` | 16 MiB | 프레임 상한(길이 필드 제외) |
| `handshake_ms` | 10000 | 새 접속이 hello를 보내야 하는 기한 |

메서드 이름: `register_claimable`, `register_dispatch`, `claim`, `complete`, `delete`, `subscribe`, `unsubscribe`.
채널 패턴: 정확한 이름, `*`로 끝나는 접두사, 또는 `*`.

토큰은 설정 파일에 평문으로 있다. 파일 권한으로 보호하고 저장소에 커밋하지 않는다.

## 구조

| 경로 | 내용 |
| --- | --- |
| `src/config/` | JSON 설정 → `EngineConfig`·권한표·자격 목록 |
| `src/connection/` | 접속 하나: 핸드셰이크, 읽기 태스크(프레임 → `Session::call`), 쓰기 태스크(`Outbound` → 프레임) |
| `src/lib.rs` | `run`: 저널 복구, 엔진 시작, 수신, 종료·장애 전파 |
| `src/main.rs` | 바이너리 |
| `tests/remote.rs` | 실제 TCP 위의 서버와 `ruvent_client::remote` 클라이언트 |

엔진은 소켓을 기다리지 않는다. 쓰기 태스크의 송신 버퍼에 `try_send`로 넘기고 지나간다. 엔진이 접속을
끊으면(버퍼 포화, 종료) 쓰기 태스크가 끝나고 소켓이 닫힌다. 클라이언트에게는 연결 종료가 손실 신호다.

## 테스트

```sh
cargo test -p ruvent-server
```

| 테스트 | 판정 |
| --- | --- |
| `all_seven_methods_work_over_tcp` | 일곱 메서드가 TCP로 동작하고 효과가 맞는 접속에 닿는다 |
| `a_wrong_token_is_refused_with_its_reason` | 모르는 토큰은 서버 사유와 함께 거절 |
| `a_credential_may_use_only_its_granted_methods_and_channels` | 자격별 메서드·채널 권한, 위임 |
| `a_backlog_larger_than_one_page_is_read_to_the_end_by_subscribing_again` | 한 응답보다 큰 대기분을 구독 재호출로 끝까지 |
| `local_and_remote_connections_see_the_same_results_in_the_same_order` | 같은 입력에 로컬·원격의 응답·전달·순서가 같다 |
| `a_claim_held_by_a_disconnected_remote_worker_is_reclaimed_by_the_next_registration` | 끊긴 원격 워커의 착수가 다음 등록으로 회수되어 다른 워커가 새 id로 처리 |
| `a_resent_completion_after_a_lost_reply_has_no_effect_and_no_follow_ups` | 응답 유실 뒤 재전송은 무효, 후속 0건 |
| `after_a_server_restart_a_surviving_workers_old_completion_is_void` | 서버 재기동 뒤 살아 있던 워커의 옛 완료는 무효 |
| `when_the_server_goes_away_the_inbox_closes_and_calls_fail_closed` | 서버가 접속을 놓으면 받은 편지함이 닫히고 이후 호출은 매달리지 않고 `Closed` |
| `the_example_configuration_parses_into_grants`, `bad_configurations_are_refused_with_a_reason` | 설정 해석과 거절 사유 |
