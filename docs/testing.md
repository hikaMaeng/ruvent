# 테스트

이 라이브러리는 기능이 자주 바뀌지 않는다. 그래서 테스트망은 한 번 촘촘히 짜 두고, 그 뒤의 모든 변경을
이 망에 통과시키는 것을 전제로 한다.

## 실행

```sh
cargo xtask verify            # 모든 관문. 변경은 이것이 통과해야 끝난다
cargo xtask verify --deep     # 위 + 속성 테스트를 사례 10만 개로
cargo xtask mutants           # 핵심 파일의 변이 테스트 (cargo-mutants 필요)
```

마지막 실행 기록: [reports/test-2026-11-22.txt](reports/test-2026-11-22.txt) (`verify --deep`, 모든 관문 통과).

`verify`는 아래 순서로 돌고 처음 실패한 곳에서 멈춘다. 각 관문은 `gate name=… status=ok|failed` 한 줄을 남긴다.

| 관문 | 명령 | 막는 것 |
| --- | --- | --- |
| format | `cargo fmt --check` | 포맷 차이 |
| lint | `cargo clippy --workspace --all-targets -D warnings` | 경고 전부. 워크스페이스 린트(`unsafe` 금지, await를 넘는 잠금, 라이브러리의 터미널 출력 등) 포함 |
| lint without json | `--no-default-features` | `json` 기능을 끈 빌드 |
| api docs | `cargo doc` + `RUSTDOCFLAGS=-D warnings` | 깨진 문서 내부 링크 |
| static | `--test static` | 아래 정적 수준 |
| compile-fail pairs | `--doc` | 컴파일되면 안 되는 코드 |
| state machine | `--lib` (기능 켬·끔 두 번) | 상태 기계 속성 테스트 |
| runtime | `--test runtime` | 아래 런타임 수준 |
| acceptance | `--test acceptance` | 아래 인수 수준 |

갱신이 필요한 고정 파일은 둘이고, 둘 다 `RUVENT_BLESS=1`로 다시 쓴다. 다시 쓰기 전에 차이를 읽어야 한다.

* `crates/ruvent/tests/static/api.txt` — 공개 API
* `crates/ruvent/tests/runtime/fixtures/journal-v1.bin` — 저널 파일 형식. 이것이 바뀌면 형식 버전을 올리고 옛 저널을 어떻게 할지 정해야 한다

## 세 수준

| 수준 | 묻는 것 | 판정 근거 | 위치 |
| --- | --- | --- | --- |
| 정적 | 실행하지 않고 소스와 타입만으로 정해지는 구조 결정이 지켜지는가 | 구조 규칙, 공개 API 고정본, 컴파일러 | `tests/static/`, `src/compile_fail.rs` |
| 런타임 | 실제로 돌렸을 때 기계가 맞게 동작하는가 | 모델, 불변식, 재생 동치 | `src/engine/*/tests.rs`, `tests/runtime/` |
| 인수 | 요구한 것이 충족되는가 | **요구사항 원문**. 테스트마다 원문을 인용한다 | `tests/acceptance/` |

인수 테스트는 공개 API만 쓴다. 같은 변경에서 쓴 테스트로 그 변경을 증명하지 않도록, 판정 기준을 테스트
옆의 설명이 아니라 사용자가 준 요구 문장에 둔다.

## 정적 수준

### 구조 ([`tests/static/architecture.rs`](../crates/ruvent/tests/static/architecture.rs))

| 결정 | 테스트 | 검사 방법 |
| --- | --- | --- |
| 모듈은 아래로만 의존한다 | `modules_depend_only_downward` | 소스의 `crate::` 경로를 계층표와 대조. `super::super` 금지 |
| 모든 모듈이 계층표에 있고 표에 죽은 항목이 없다 | `every_module_is_in_the_layer_table_and_no_layer_is_stale` | 디렉터리와 표의 집합 비교 |
| 폴링하지 않고 타이머는 선언된 셋뿐이다 | `nothing_polls_and_every_timer_is_declared` | `interval`, `thread::sleep`, `yield_now`, `try_recv` 등 금지. `time::sleep`·`time::timeout` 위치와 개수를 표와 대조 |
| 저널은 한 곳에서만 쓴다 | `the_journal_is_written_from_one_place` | `journal.append`·`rewrite`·flight 생성·writer 채널 용량 1의 호출처 개수 |
| 엔진 상태의 소유자는 하나다 | `the_engine_state_has_one_owner` | `State::new`, 엔진 태스크 생성, 로그 쓰기 잠금 위치 |
| `unsafe`·터미널 출력·`todo!`가 없다 | `the_library_has_no_unsafe_and_writes_nothing_to_the_terminal` | 주석을 뺀 소스 검색 |
| JSON은 wire만 안다 | `only_the_wire_knows_json` | `serde_json` 사용 파일과 기능 게이트 |
| 벽시계는 한 번만 읽는다 | `the_wall_clock_is_read_once` | `SystemTime::now` 위치 |
| 런타임 의존성은 선언된 목록이다 | `runtime_dependencies_are_the_declared_set` | `Cargo.toml`의 `[dependencies]`와 대조 |
| 소스는 제어 문자 없는 UTF-8이다 | `source_files_are_plain_utf8_text` | NUL·CR 등 바이트 검사 |
| 주석 제거기가 코드와 문자열을 보존한다 | `the_comment_stripper_keeps_code_and_strings` | 위 검사들이 쓰는 도구 자체의 검사 |

### 공개 API ([`tests/static/api.rs`](../crates/ruvent/tests/static/api.rs))

| 결정 | 테스트 | 검사 방법 |
| --- | --- | --- |
| 공개 표면은 커밋된 고정본과 같다 | `the_public_api_is_the_committed_one` | `syn`으로 모든 `pub` 항목의 시그니처·필드·변형·트레이트 구현을 한 줄씩 뽑아 `api.txt`와 비교. 사라진 줄과 생긴 줄을 보고 |

### 타입 계약 ([`tests/static/contracts.rs`](../crates/ruvent/tests/static/contracts.rs), [`src/compile_fail.rs`](../crates/ruvent/src/compile_fail.rs))

| 약속 | 테스트 | 검사 방법 |
| --- | --- | --- |
| 핸들은 스레드 사이에서 공유된다 | `handles_are_shareable_across_threads` | `Send + Sync + 'static` 경계로 컴파일 |
| 값은 호출자가 기대는 트레이트를 갖는다 | `values_have_the_traits_callers_rely_on` | `Copy`, `Error`, `Serialize + DeserializeOwned`, `From<Lost>` |
| 모든 async 호출을 spawn할 수 있다 | `every_async_call_can_be_spawned` | 엔진·구독·게이트웨이·앱의 모든 future가 `Send`인지 컴파일러로 확인 |
| 평범한 async 클로저가 핸들러다 | `a_closure_is_a_handler` | 등록이 컴파일된다 |
| 컴파일되면 안 되는 것 | 문서 테스트 쌍 5개 | 시도 번호 위조(`Context::new`), `EventId::positional`, `engine::state` 접근, `Rc`를 쥔 핸들러, `Proposal.principal`. 각각 금지 줄만 뺀 쌍둥이가 컴파일되어야 한다 |

### 문서 ([`tests/static/docs.rs`](../crates/ruvent/tests/static/docs.rs))

| 결정 | 테스트 | 검사 방법 |
| --- | --- | --- |
| 모든 테스트가 이 문서에 있고, 이 문서는 테스트만 가리킨다 | `every_test_is_on_the_map_and_the_map_names_only_tests` | 테스트 함수 이름과 이 문서 표의 이름을 양방향 대조 |
| 인수 테스트는 요구 원문을 인용한다 | `every_acceptance_test_quotes_its_requirement` | 문서 주석의 `/// >` 인용 |
| 문서의 상대 링크가 모두 존재한다 | `document_links_resolve` | `README.md`, `docs/*.md` |
| 문서는 제어 문자 없는 UTF-8이다 | `documents_are_plain_utf8_text` | NUL·CR 등 바이트 검사 |

## 런타임 수준

### 상태 기계 ([`src/engine/state/tests.rs`](../crates/ruvent/src/engine/state/tests.rs))

엔진 태스크가 하듯 `State`를 직접, 동기로 몬다. 런타임도 디스크도 없으므로 무작위 시퀀스 수천 개가 몇 초에 끝난다.

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 저널을 재생하면 언제나 지속 상태가 그대로 돌아온다 | `the_journal_always_replays_to_the_durable_state` | 제출·꺼냄·시작·완료·반납·사실·진행·시간 경과·쓰기·압축·**크래시 재시작**을 무작위로 섞은 최대 160단계, 기본 2,048개(`--deep` 10만 개). 쓰기마다 "지금까지의 저널 재생 결과 = 살아 있는 상태의 지속 부분". 매 단계: 실행당 소유자 1, 쥔 사람 없는 메시지 없음, 밀려난 시도의 연산은 아무것도 바꾸지 않음, 위치 단조 증가, 로그의 이벤트 중복 없음, 큐 안에서 메시지·dead letter id 중복 없음, 보존 바이트 = 남은 이벤트 비용의 합, 중복 제거 표 = 남은 이벤트, 재시작 뒤 독자에게 게시될 로그 = 재생된 로그, 쓸 레코드가 있으면 그 전에 dirty였음, 이미 버려진 이벤트는 쓰지 않음, 한 번의 쓰기에 같은 실행의 `Exec`는 하나, `live_records()` = 스냅샷 크기 |
| 같은 이벤트를 실은 두 번째 메시지는 버려지고 방치되지 않는다 | `a_second_message_carrying_the_same_event_is_dropped_not_stranded` | 위 속성이 찾은 최소 시퀀스 |
| 잊힌 뒤 다시 시작된 키는 재시작을 견딘다 | `a_key_started_again_after_it_was_forgotten_survives_a_restart` | 위 속성이 찾은 최소 시퀀스 |
| 보존 한도로 빠졌다 다시 기록된 이벤트는 한 번만 재생된다 | `an_event_recorded_again_after_retention_dropped_it_is_replayed_once` | 위 속성이 찾은 최소 시퀀스 |
| 재시작 뒤 새 메시지는 dead letter의 id를 받지 않는다 | `a_restart_never_reuses_a_dead_letters_id` | 가장 큰 id가 dead letter인 저널에서 재시작 (변이 테스트가 찾은 빈틈) |
| 시도 번호는 재시작을 넘어서도 커지기만 한다 | `attempt_numbers_only_grow_across_restarts` | 외부 펜싱 토큰으로 쓸 수 있어야 한다 (변이 테스트가 찾은 빈틈) |
| 두 번 올라간 키는 한 번의 쓰기에 한 번만 기록된다 | `a_key_listed_twice_is_stated_once` | 쓰기 대기 중 만료된 실행과 같은 키의 새 실행 (불변식이 찾은 결함) |
| 오래된 만료 항목이 더 새로운 완료를 지우지 않는다 | `an_old_expiry_entry_does_not_take_a_newer_settlement_with_it` | 재생 뒤 같은 키의 만료 항목 둘. 5ms 전에 끝난 실행은 창 안이므로 재제출이 실행되지 않음 (변이 테스트가 찾은 빈틈) |

### 큐 ([`src/engine/queue/tests.rs`](../crates/ruvent/src/engine/queue/tests.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 큐는 단순하게 쓴 모델과 똑같이 동작한다 | `the_queue_behaves_like_its_model` | 메시지마다 상태 하나만 두는 모델과 4,096개 시퀀스로 비교. 꺼내는 메시지와 순서·읽힌 횟수가 같고, 깨워 달라는 시각이 모델의 반환 시각보다 늦지 않다 |
| 기다리는 소비자가 없으면 깨움을 요청하지 않는다 | `with_nobody_waiting_the_queue_asks_for_no_wakeup` | `next_deadline()`이 `None` |

### 읽기 로그 ([`src/engine/log/tests.rs`](../crates/ruvent/src/engine/log/tests.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 비워진 채널은 기다리는 사람이 있을 때만 남는다 | `an_emptied_channel_is_kept_only_while_someone_waits_on_it` | 살아 있는 깨우개의 채널은 남고, 죽은 깨우개뿐인 채널은 지워짐. 공개 API로 보이지 않는 메모리 한도 (변이 테스트가 찾은 빈틈) |
| 조용한 채널에 구독을 열고 닫기를 반복해도 쌓이지 않는다 | `reopening_subscriptions_on_a_quiet_channel_does_not_pile_up` | 롱폴링처럼 100번 열고 닫은 뒤 깨우개는 살아 있는 1개뿐 (변이 테스트가 찾은 빈틈) |

### 엔진 ([`tests/runtime/engine.rs`](../crates/ruvent/tests/runtime/engine.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 파생 식별자는 안정적이고 부분이 섞이지 않는다 | `derived_ids_are_stable_and_do_not_run_parts_together` | `["ab","c"]` ≠ `["a","bc"]`, 반응자별 식별자가 다름 |
| 요청한 visibility 기한이 지나면 메시지가 스스로 돌아온다 | `a_visibility_deadline_returns_an_unfinished_message_by_itself` | 시계를 멈춰 놓고 기한만 움직인다 |
| 기한 없이 든 메시지는 돌려줄 때만 돌아온다 | `a_held_message_comes_back_only_when_it_is_given_back` | 하루가 지나도 안 돌아옴. 지연 반환은 29초엔 없고 30초에 있음 |
| 유휴 엔진과 주차된 소비자는 깨어나지 않는다 | `an_idle_engine_and_its_parked_consumers_do_not_wake` | 1시간 동안 `Stats::wakeups` 불변 |
| 동시 소비자가 메시지를 공유하지 않는다 | `concurrent_consumers_never_share_a_message` | 스레드 8, 소비자 16, 메시지 20,000. id별 전달 횟수가 전부 1 |
| 이벤트 하나는 워커 하나가 실행하고 한 번 답한다 | `one_event_is_run_by_one_worker_and_answered_once` | 실행 중 재제출은 `Duplicate`, 종료 후는 `Settled`, 내용이 다르면 `Conflict` |
| 밀려난 시도는 쓰지 못하고 시도 횟수는 유한하다 | `a_superseded_attempt_cannot_write_and_attempts_are_bounded` | 옛 시도의 owns·emit·finish·release가 전부 `false`. 소진 시 dead letter |
| 반응자마다 자기 이벤트를 같은 스텝에 발급받는다 | `each_reactor_is_issued_its_own_event_in_the_same_step` | 큐 둘에 서로 다른 `transaction_key` |
| 구독은 커서에서 재개하고 워커 트래픽을 보지 않는다 | `a_subscription_resumes_from_its_cursor_and_never_sees_worker_traffic` | 채널 둘 병합 순서, `From::Now`, 재개 |
| 로그는 자라는 순간에 한정되고 뒤처진 독자는 통보받는다 | `the_log_is_bounded_as_it_grows_and_a_reader_left_behind_is_told` | 상한 4에서 10건 append 후 4건만 남음 |
| 로그는 바이트로도 한정된다 | `the_log_is_bounded_by_bytes` | 이벤트당 payload + 256바이트. 3건 분량 한도에 1,000바이트 이벤트 5건 → 최신 3건, `retained_bytes` 정확 (변이 테스트가 찾은 빈틈) |
| 마지막으로 버려진 이벤트에 선 독자는 놓친 것이 없다 | `a_reader_at_the_last_dropped_event_has_missed_nothing` | 상한 2에서 4건. e1 커서는 `lagged` 아님, e0 커서는 `lagged` (변이 테스트가 찾은 빈틈) |
| 보존 한도가 비운 채널에서도 기다리는 독자는 깨어난다 | `a_reader_is_woken_on_a_channel_retention_emptied` | 상한 1. 대기 중인 채널의 유일한 이벤트가 밀려난 뒤 새 이벤트가 오면 깨어남 (변이 테스트가 찾은 빈틈) |
| 새 구독이 기다리던 구독을 떼어 내지 않는다 | `a_new_reader_does_not_unhook_one_already_waiting` | 대기 중인 채널에 두 번째 구독을 연 뒤 이벤트가 오면 둘 다 받음 (변이 테스트가 찾은 빈틈) |
| 멈춘 엔진은 모든 호출을 거부하고 이유를 말한다 | `a_stopped_engine_refuses_and_says_why` | 종료 뒤 submit·dequeue·flush가 오류, 메시지가 정확함 (변이 테스트가 찾은 빈틈) |
| 진행 이벤트는 inbox가 찼을 때만 버려지고 버린 수만 세어진다 | `progress_is_dropped_and_counted_only_when_the_inbox_is_full` | 용량 4의 inbox에 10건: 앞 4건 성공, 6건 실패, `progress_dropped` = 6 (변이 테스트가 찾은 빈틈) |
| 시각 없이 제출한 명령은 엔진 시계로 찍힌다 | `a_command_without_a_time_is_stamped_on_submission` | `created_at` 0은 현재 시각으로, 지정한 값은 그대로 (변이 테스트가 찾은 빈틈) |
| 끝난 실행은 중복 제거 창 동안만 기억된다 | `a_settled_execution_is_remembered_only_for_its_window` | 창 1초. 창 안의 재제출은 `Settled`, 창이 지난 뒤 다음 완료가 일어나면 같은 키가 새로 실행됨 (변이 테스트가 찾은 빈틈) |

### 저널 ([`tests/runtime/journal.rs`](../crates/ruvent/tests/runtime/journal.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 재시작은 빚진 일과 시도 횟수를 되찾는다 | `a_restart_gets_back_the_work_still_owed_and_how_often_it_was_tried` | 끝난 것은 없고 안 끝난 둘이 재전달, 시도 횟수 2, 에폭만큼 오른 위치, 옛 커서는 `lagged` |
| 두 쓰기 사이에 왔다 간 일은 적지 않는다 | `work_that_comes_and_goes_between_two_writes_is_never_written` | 1,000건 완료 + 1건 대기 뒤 flush → 레코드 1개 |
| 크래시는 마지막 완료 쓰기 이후만 잃는다 | `a_crash_loses_only_what_changed_after_the_last_completed_write` | 실행 중 저널 파일을 복사해 연다 |
| 끝난 결과와 이벤트는 요청한 곳만 남는다 | `settled_results_and_events_survive_a_restart_only_where_asked` | 지정 채널의 사실만 복원, 진행 이벤트는 없음, 재제출은 `Settled` |
| transient 큐는 저널에 언급되지 않는다 | `a_transient_queue_is_never_mentioned_to_the_journal` | 레코드 증가 0 |
| 찢어진 꼬리는 잘리고 앞은 보존된다 | `a_torn_tail_is_cut_off_and_everything_before_it_survives` | 반쪽 프레임을 덧붙이고 다시 연다 |
| 저널은 두 번 열리지 않고 순서 밖 쓰기를 거부한다 | `a_journal_cannot_be_opened_twice_or_written_out_of_turn` | 두 번째 open은 `WouldBlock`, 건너뛰기·덮어쓰기 배치는 오류 |
| 저널 쓰기는 겹치지 않는다 | `journal_writes_never_overlap_however_eagerly_they_are_wanted` | 3ms 저널, 창 0, 압축 임계 64. 동시 진행 최대 1 |
| 저널은 대부분 대체되면 스스로 접는다 | `the_journal_folds_itself_when_it_is_mostly_superseded` | 압축 발생, 파일이 최대치보다 작아짐 |
| 요청하면 지금 접는다 | `an_explicit_compaction_folds_the_journal_now` | 레코드 100여 개가 에폭과 남은 메시지 2개로, 파일이 1/4 미만으로, 재시작 뒤 남은 일 보존 (변이 테스트가 찾은 빈틈) |
| 임계 아래의 저널은 접지 않는다 | `a_journal_under_its_threshold_is_not_folded` | 임계 61, 비율 1. 레코드가 정확히 61개(60개 대체)여도 압축 0회. 임계는 "넘을 때"다 (변이 테스트가 찾은 빈틈) |
| 운영 중에도 저널은 임계를 크게 넘지 않는다 | `a_running_journal_never_grows_far_past_its_threshold` | 임계 100. 매 flush 뒤 파일 복사본의 레코드 수가 150 이하, 크래시 시점 크기의 한도 (변이 테스트가 찾은 빈틈) |
| 대체된 것이 없는 저널은 다시 쓰지 않는다 | `a_journal_with_nothing_superseded_is_never_rewritten` | 비율 1, 임계 20. 살아 있는 레코드 41개뿐인 저널은 운영 중에도 종료 때도 압축 0회 (변이 테스트가 찾은 빈틈) |
| 정상 종료는 임계를 넘은 저널만 접는다 | `a_clean_stop_folds_only_a_journal_over_its_threshold` | 임계 61에 정확히 61개면 그대로. 141개 중 21개만 유효하고 운영 중 판단 주기에 걸리지 않은 저널은 종료 때 21개로 (변이 테스트가 찾은 빈틈) |
| 저널 쓰기 실패는 엔진을 멈추고 이유를 알린다 | `a_failed_journal_write_stops_the_engine_and_says_why` | 두 번째 쓰기가 실패하는 저널. flush는 오류, 이후 호출은 "journal write failed: …"로 거부 (변이 테스트가 찾은 빈틈) |
| 엄격 모드는 디스크에 있는 것만 응답한다 | `strict_mode_acknowledges_only_what_is_on_disk` | 응답 직후 복사한 이미지에 50건 전부 |
| flush는 그 변화가 디스크에 닿은 뒤에야 끝난다 | `a_flush_resolves_only_once_its_changes_are_on_disk` | 쓰기가 막힌 저널. 상쇄된 변화의 flush는 즉시, 남은 변화의 flush는 1시간이 지나도 안 끝나다가 쓰기가 끝나면 끝남 (변이 테스트가 찾은 빈틈) |

### 파일 형식 ([`tests/runtime/file_format.rs`](../crates/ruvent/tests/runtime/file_format.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 저널이 아닌 파일은 거부하고 건드리지 않는다 | `a_file_that_is_not_a_journal_is_refused_and_left_alone` | CSV 파일과 헤더보다 짧은 `hello`를 열면 `InvalidData`, 바이트 불변 |
| 찢어진 헤더는 빈 저널이다 | `a_torn_header_is_an_empty_journal` | 헤더를 모든 길이로 잘라 연다 |
| 파일 형식은 고정되어 있다 | `the_file_format_is_fixed` | 레코드 종류 전부·선택 필드 있음/없음을 담은 고정본과 바이트 비교, 고정본을 다시 읽어 같은 레코드 |
| 어디가 손상되든 쓴 것의 앞부분만 나온다 | `damage_anywhere_yields_a_prefix_of_what_was_written` | 모든 바이트 위치에서 자르기, 모든 바이트의 비트 3개 뒤집기(약 3,800회). 복원 결과가 쓴 배치들의 앞부분 중 하나이거나 헤더 손상으로 인한 `InvalidData` |
| CRC를 맞춘 너무 짧은 프레임도 손상으로 잘린다 | `a_crafted_frame_too_short_for_its_header_is_cut_off_not_read` | 본문 5바이트에 CRC가 맞는 조작 프레임. 패닉 없이 헤더까지 잘림 (변이 테스트가 찾은 빈틈) |
| 찢어진 꼬리 뒤에 쓴 것은 다음 재시작에서 살아남는다 | `writes_after_a_torn_tail_survive_the_next_restart` | 꼬리를 자르지 않으면 새 프레임이 쓰레기 뒤에 붙어 다음 열기에서 사라진다 (변이 테스트가 찾은 빈틈) |

### wire ([`tests/runtime/wire.rs`](../crates/ruvent/tests/runtime/wire.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 악의적 입력에 패닉하지 않는다 | `hostile_input_never_panics` | 임의 문자열 2,048개 |
| 모양이 비슷한 프레임에도 패닉하지 않는다 | `almost_right_frames_never_panic` | 임의 JSON에 `type`만 붙인 프레임 |
| 제안은 모든 필드를 지닌 채 건너간다 | `a_proposal_crosses_the_wire_intact` | 임의 JSON payload가 같은 값으로 도착 |
| 모든 커서가 정밀도를 잃지 않는다 | `every_cursor_survives_the_wire` | 임의 `u64`, 2^53 초과 포함 |
| 모든 이벤트가 JSON으로 인코딩된다 | `every_event_encodes_as_json` | JSON payload는 값 그대로, 아니면 문자열 |
| 서버 프레임 이름은 고정되어 있다 | `server_frame_names_are_fixed` | 다른 언어의 전송이 읽는 문자열 |
| 커서 없는 구독의 시작점 | `a_subscription_without_a_cursor_starts_now_or_at_the_start` | `now`, `start`, 커서 우선, 음수 거부 |
| `null`은 payload이고 없는 payload는 거부된다 | `a_null_payload_is_a_payload_and_a_missing_one_is_refused` | 회귀 고정 |

### 프레임워크 ([`tests/runtime/app.rs`](../crates/ruvent/tests/runtime/app.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 리액션 연쇄에 함수 간 호출이 없다 | `a_chain_of_reactions_runs_with_no_handler_naming_the_next` | 함수 셋이 사실만 기록, 델타 순서 보존, 미등록 action 거부 |
| panic은 재시도, 치명 실패는 기록 | `a_panic_is_retried_and_a_fatal_failure_is_recorded` | 실행 횟수 2, `.failure` 이벤트 |
| 기한을 넘긴 스텝은 버려지고 다시 실행된다 | `a_step_that_overruns_is_dropped_and_retried` | 소유 값의 `Drop` 1회, 경과 시간이 정확히 30초 + 2초 |
| 유휴 서비스는 엔진을 깨우지 않는다 | `idle_services_do_not_wake_the_engine` | 6시간 동안 `wakeups` 불변 |

## 인수 수준

판정 기준은 요구 원문이다. 테스트 파일에는 원문 전체를 인용했고, 여기에는 줄여서 적는다.
"다루지 않는 범위"는 요구가 테스트보다 넓은 부분이다.

### 요구사항 ([`tests/acceptance/requirements.rs`](../crates/ruvent/tests/acceptance/requirements.rs))

| 요구 원문 (요약 인용) | 테스트 | 관측 방법 | 다루지 않는 범위 |
| --- | --- | --- | --- |
| "양방향 프로토콜은 플러그인 계층이 되길 원해. http롱폴링, 웹소켓, webRTC 등을 기본 지원하고…" | `a_protocol_is_a_plugin_written_against_public_traits` | 크레이트 밖에서 공개 트레이트만으로 쓴 메모리 전송이 인증 거부, 구독, 제출, 연쇄 스트리밍, 커서 재개(누락·중복 없음)를 한다 | **HTTP 롱폴링·WebSocket·WebRTC 기본 전송은 아직 없다** |
| "이벤트는 반드시 하나의 워커가 가져간다… 이벤트 자체를 여러개 발급해야해" | `one_event_is_taken_by_one_worker_and_fan_out_issues_separate_events` | 주문 100건 × 같은 키로 동시 3회 제출. 접수 핸들러 정확히 100회, 반응 서비스 둘이 주문마다 1회씩, 이벤트 id 200개가 모두 다름 | 여러 프로세스 |
| "이전 디스크 저널링이 진행중이면 덮어쓰지 않게 단지 시간만을 안전장치로 두면 안된다" | `a_write_in_progress_is_never_overtaken_whatever_the_clock_says` | 허가가 있어야 끝나는 저널. 쓰기가 막힌 동안 제출 1,000건이 응답되고, 창 10ms에 1시간이 지나도 두 번째 쓰기가 시작되지 않으며, 첫 쓰기가 끝나는 순간 다음이 사슬을 이어 시작된다 | |
| "쓰레드를 깨우는 것조차 쓰레드 루프를 도는게 아니라 OS활성신호로 깨울거잖아" | `an_idle_application_with_a_connected_client_does_not_wake` | 서비스·전송·구독 중인 클라이언트가 붙은 채 24시간 동안 `wakeups` 불변. 다음 이벤트로 깨어남 | 워커 런타임 바깥(전송 플러그인 내부)의 깨움 |
| "완료된 이벤트의 로그는… 너무 많이 쌓이는 것도 곤란해" | `completed_work_does_not_pile_up_in_memory_or_on_disk` | 실제 파일 저널에서 명령 20,000건 완료. 메모리 이벤트 ≤ 2,000(보존 한도), 정상 종료 뒤 저널 레코드 ≤ 2,000(압축 임계), 빚진 일 0 | 크래시 직후의 저널 크기 (임계 + 마지막 접기 이후 분량) |
| "저널링은 개별 레코드보다 스냅샷으로 취합하여 남기는게 더 나은거 아닌가. 복원하는데도 문제는 없잖아." | `a_folded_journal_restores_exactly_what_is_owed` | 2,000건이 오가며 압축이 일어난 저널의 크래시 시점 이미지가 대기 300건을 순서대로 정확히 복원 | |
| "프레임웍 설정수준에서 저널링을 않는 채널을 설정할 수도 있잖아" | `channels_and_services_can_be_kept_off_the_journal` | `persist_events(Except(["live."]))`와 `.transient()` 서비스. 재시작 뒤 `audit.` 이벤트와 일반 서비스의 일은 남고 `live.` 이벤트와 transient 서비스의 일은 없음 | |
| "스프링 프레임웍이… 손쉽게 원하는 서버로 확장가능해야한다" | `a_feature_installs_itself_as_a_module` | 모듈 둘이 서로를 지목하지 않고 사실로 이어짐. 명령 하나를 두 모듈이 받으면 시작 거부 | |
| "이 러스트 라이브러리는 독립된 서버로 동작하는게 아니라 러스트 라이브러리로 활용되길 원한다." | `the_engine_alone_is_usable_without_the_framework` | 프레임워크·전송·포트 없이 엔진 API만으로 명령 처리 | |

### 프로세스 강제 종료 ([`tests/acceptance/crash.rs`](../crates/ruvent/tests/acceptance/crash.rs))

테스트 바이너리가 자기 자신을 자식 프로세스로 띄우고(`child`), 준비 신호를 받은 뒤 죽인다(Windows
`TerminateProcess`, 그 밖에서는 `SIGKILL`). 소멸자도 flush도 잠금 해제도 없다. 프로세스 크래시이지 전원
차단이 아니다. 운영체제가 받아 둔 쓰기는 남는다.

| 요구 원문 | 테스트 | 관측 방법 |
| --- | --- | --- |
| "하지만 인메모리만으로 운영되면 안된다." | `a_killed_process_leaves_every_accepted_command_and_every_attempt` | 엄격 모드, 실행이 끝나지 않는 핸들러. 접수한 20건 전부가 남고, 실행 중이던 8건은 시도 2회째로 세어지며, 죽은 프로세스의 저널 잠금이 풀려 있다 |
| "따라서 100% 장애대응을 대비하지 않는다." | `a_killed_process_loses_at_most_what_came_after_the_last_write` | 기본 모드. 300건 flush 뒤 300건 더. 죽는 순간 적히지 않은 변화가 있었음을 확인한 뒤, flush한 300건은 순서대로 전부 남고, 남은 것은 제출 순서의 앞부분이며, 제출하지 않은 것은 없다 |
| (자식 프로세스 진입점) | `child` | 환경 변수가 있을 때만 시나리오를 돈다. 없으면 빈 테스트 |

## 이 테스트망이 찾은 결함

테스트를 먼저 쓰고, 실패를 확인한 다음 고쳤다. 회귀 테스트는 수정을 하나씩 되돌렸을 때 실패하는지 확인했다.

| 결함 | 찾은 테스트 | 결과였을 일 | 수정 |
| --- | --- | --- | --- |
| 중복 제거 기간이 지나 잊힌 키가 다음 쓰기 전에 다시 시작되면, 저널에 새 `Exec` 뒤에 `Forget`이 적혔다 | 재생 동치 속성 | 재시작 뒤 그 실행의 시도 횟수가 사라짐 | `Forget`을 먼저 적는다 |
| 같은 `event_id`를 실은 두 번째 메시지를 `Duplicate`로 판정하고도 큐에 남겼다 | 재생 동치 속성의 "쥔 사람 없는 메시지" 불변식 | 그 메시지가 재시작 전까지 영원히 in-flight | 실행 중인 바로 그 메시지만 남긴다 |
| 보존 한도로 빠졌다 다시 기록된 사실이 저널에 둘 있으면 재시작 때 둘 다 재생됐다 | 재생 동치 속성의 "중복 이벤트 없음" 불변식 | 독자가 같은 이벤트를 두 번 받음 | 재생 시 나중 기록이 앞선 기록을 대체 |
| 쓰기 대기 중 만료된 실행의 키로 새 실행이 생기면, 한 번의 쓰기에 같은 `Exec`가 두 번 실렸다 | 재생 동치 속성에 추가한 "한 번의 쓰기에 `Exec`는 하나" 불변식 | 디스크 낭비, `journal_lag` 과대 계상. 재생 결과는 같음 | 이미 기록한(dirty가 풀린) 실행은 건너뜀 |
| `"payload": null`인 이벤트 프레임을 거부했다 | `a_proposal_crosses_the_wire_intact` | 정상 요청이 조용히 버려짐 | `null`은 값, 필드 없음만 거부 |
| 저널이 아닌 파일을 열면 0바이트로 잘랐다 | `a_file_that_is_not_a_journal_is_refused_and_left_alone` | 경로를 잘못 주면 남의 데이터가 사라짐 | 파일 헤더(이름+형식 버전). 다른 파일은 거부 |
| 압축 판단이 스냅샷에 쓰지 않는 것(메모리 전용 완료 실행, 저장 대상이 아닌 이벤트)까지 셌고, 정상 종료 때는 압축하지 않았다 | `completed_work_does_not_pile_up_in_memory_or_on_disk` | 명령 20,000건 완료 뒤 저널 레코드 32,957개. 판단을 고친 뒤에도 종료 시 6,319개 | 스냅샷이 쓸 것만 센다(속성 테스트가 스냅샷 크기와 같은지 확인). 임계의 절반만큼 쓸 때마다 한 번 계산. 종료 때 임계를 넘었으면 접는다 |

부수 개선: 전송 플러그인 작성자가 `async_trait` 버전을 맞추지 않아도 되도록 `ruvent::async_trait`을 재수출했다.
인수 테스트의 메모리 전송을 공개 API만으로 쓰다가 드러났다.

### 기각된 가설과 테스트 쪽 오류

* 첫 속성 실패(재시작 두 번 사이에 에폭이 같아짐)는 결함이 아니었다. 엔진은 새 에폭을 디스크에 쓰기 전에는
  아무도 받지 않는다(`Core::run`의 첫 `settle`). 하네스가 그 단계를 빠뜨렸고, 하네스를 고쳤다.
* 세 번째 결함의 첫 회귀 시나리오는 수정 없이도 통과했다. 재생할 때 보존 한도가 앞선 사본을 밀어냈기 때문이다.
  저장하지 않는 채널의 이벤트로 메모리만 채우도록 시나리오를 바꾸자 수정 전 실패, 수정 후 통과가 됐다.
* 손상 시 "앞부분만 나온다"는 성질은 헤더를 추가하기 전에도 성립했다. 결함이 아니라 기존 보장의 확인이다.
* 강제 종료 테스트의 "자식이 준비 전에 종료" 실패는 하네스 문제였다. libtest가 같은 줄 앞에 테스트 이름을 찍는다.

## 변이 테스트

`cargo xtask mutants`는 `engine/state.rs`, `engine/queue.rs`, `engine/log.rs`, `engine/mod.rs`,
`journal/file.rs`의 코드를 하나씩 바꿔(조건 반전, 반환값 대체, 연산자 교체 등) 테스트가 그 변화를 잡는지 본다.
결과는 [reports/mutants-2026-11-22.md](reports/mutants-2026-11-22.md)에 있다.

| 실행 | 원문 |
| --- | --- |
| 1차 (테스트 보강 전) | `427 mutants tested in 49m: 64 missed, 279 caught, 35 unviable, 49 timeouts` |
| 2차 (놓친 것 재실행) | `199 mutants tested in 37m: 11 missed, 139 caught, 4 unviable, 45 timeouts` |

2차에서 남은 11건 중 1건은 테스트를 고쳐 막았고(수동 주입으로 확인), 10건은 동등 변이·도달 불가·이 플랫폼에서
재현 불가로 분류했다. 각각의 근거는 보고서에 있다.

## 테스트가 다루지 않는 것

* 실제 전원 차단. 강제 종료는 프로세스 크래시이고, 찢어진 꼬리와 손상은 파일 조작으로 만든 것이다.
* 저널이 다른 장치(네트워크 디스크 등)에 있을 때의 fsync 의미.
* 장시간 부하에서의 메모리 추이. 한정성은 단위로 확인했지만 몇 시간짜리 실행은 없다.
* 기본 전송 플러그인 (아직 없다).
* 여러 프로세스가 같은 엔진을 쓰는 구성 (지원하지 않는다).

## 실측

원자료: [reports/bench-2026-11-22.txt](reports/bench-2026-11-22.txt). 커밋 `91042f3`에서 측정했다. 그 뒤
압축 판단과 종료 시 압축이 바뀌었으므로, 저널 레코드 수는 지금 코드에서 다시 재야 한다. 환경과 설정은 그
파일 머리에 있다 (Threadripper 3960X, 논리 코어 48, Windows, rustc 1.96.0, 릴리스 빌드,
`EngineConfig::default()`, 페이로드 256바이트, 동시 256).

| 측정 | 모드 | 건수 | 건/초 | 저널 레코드 | 저널 쓰기 |
| --- | --- | --- | --- | --- | --- |
| 왕복 (submit → dequeue → begin → finish) | 저널 없음 | 199,936 | 149,215 | (5,939 — 버려짐) | 14 |
| 왕복 | 파일, 창 100ms | 199,936 | 153,847 | 6,531 | 14 |
| 왕복 | 파일, 엄격 | 19,968 | 39,947 | 79,873 | 537 |
| 3단 연쇄 (프레임워크) | 저널 없음 | 199,936 | 30,387 | (433,741 — 버려짐) | 63 |
| 3단 연쇄 | 파일, 창 100ms | 199,936 | 30,485 | 432,541 | 63 |
| 3단 연쇄 | 파일, 엄격 | 19,968 | 11,774 | 239,617 | 1,263 |

원본 TypeScript 구현은 같은 조건으로 측정하지 않았으므로 배수 비교는 하지 않는다.
