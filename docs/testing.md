# 테스트와 실측

## 실행

```sh
cargo test -p ruvent
cargo clippy -p ruvent --all-targets
cargo run --release --example bench -- 200000 256
```

기록: [reports/test-2026-11-22.txt](reports/test-2026-11-22.txt) — 24건 통과, 실패 0건.

## 각 테스트가 증명하는 것

판정 기준은 테스트가 아니라 요구사항이다. 왼쪽이 요구, 오른쪽이 그것을 직접 관측하는 방법이다.

### 엔진 ([`tests/engine.rs`](../crates/ruvent/tests/engine.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 파생 식별자는 안정적이고 부분이 섞이지 않는다 | `derived_ids_are_stable_and_do_not_run_parts_together` | `["ab","c"]` ≠ `["a","bc"]`, 반응자별 식별자가 다름 |
| 요청한 visibility 기한이 지나면 메시지가 스스로 돌아온다 | `a_visibility_deadline_returns_an_unfinished_message_by_itself` | 시계를 멈춰 놓고 기한만 움직인다 |
| 기한 없이 든 메시지는 돌려줄 때만 돌아온다 | `a_held_message_comes_back_only_when_it_is_given_back` | 하루가 지나도 안 돌아옴. 지연 반환은 29초엔 없고 30초에 있음 |
| 유휴 엔진과 주차된 소비자는 깨어나지 않는다 | `an_idle_engine_and_its_parked_consumers_do_not_wake` | 1시간 동안 `Stats::wakeups` 불변. 이후 submit이 해당 소비자만 깨움 |
| 동시 소비자가 메시지를 공유하지 않는다 | `concurrent_consumers_never_share_a_message` | 스레드 8, 소비자 16, 메시지 20,000. id별 전달 횟수가 전부 1 |
| 이벤트 하나는 워커 하나가 실행하고 한 번 답한다 | `one_event_is_run_by_one_worker_and_answered_once` | 실행 중 재제출은 `Duplicate`이고 별도 응답 없음. 종료 후는 `Settled`, 내용이 다르면 `Conflict` |
| 밀려난 시도는 쓰지 못하고 시도 횟수는 유한하다 | `a_superseded_attempt_cannot_write_and_attempts_are_bounded` | 옛 시도의 owns·emit·finish·release가 전부 `false`. 소진 시 dead letter |
| 반응자마다 자기 이벤트를 같은 스텝에 발급받는다 | `each_reactor_is_issued_its_own_event_in_the_same_step` | 큐 둘에 서로 다른 `transaction_key`. 같은 사실 재기록은 아무것도 만들지 않음 |
| 구독은 커서에서 재개하고 워커 트래픽을 보지 않는다 | `a_subscription_resumes_from_its_cursor_and_never_sees_worker_traffic` | 채널 둘 병합 순서, `From::Now`, 재개 |
| 로그는 자라는 순간에 한정되고 뒤처진 독자는 통보받는다 | `the_log_is_bounded_as_it_grows_and_a_reader_left_behind_is_told` | 상한 4에서 10건 append 후 4건만 남음. 나이 한도는 다음 append 때 적용 |

### 저널 ([`tests/journal.rs`](../crates/ruvent/tests/journal.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 재시작은 빚진 일과 시도 횟수를 되찾는다 | `a_restart_gets_back_the_work_still_owed_and_how_often_it_was_tried` | 끝난 것은 없고 안 끝난 둘이 재전달. 재시작 전 시도가 세어져 2. 위치는 에폭만큼 올라감. 옛 커서는 `lagged` |
| 두 쓰기 사이에 왔다 간 일은 적지 않는다 | `work_that_comes_and_goes_between_two_writes_is_never_written` | 1,000건을 제출·완료하고 1건을 남긴 뒤 flush → 레코드 1개만 증가 |
| 크래시는 마지막 완료 쓰기 이후만 잃는다 | `a_crash_loses_only_what_changed_after_the_last_completed_write` | 실행 중 저널 파일을 복사해 "크래시 시점 이미지"로 연다 |
| 끝난 결과와 이벤트는 요청한 곳만 남는다 | `settled_results_and_events_survive_a_restart_only_where_asked` | 지정 채널의 사실만 복원, 진행 이벤트는 지정 채널이어도 없음, 재제출은 `Settled` |
| transient 큐는 저널에 언급되지 않는다 | `a_transient_queue_is_never_mentioned_to_the_journal` | 메시지와 실행이 있어도 레코드 증가 0. 재시작 뒤 그 큐만 비어 있음 |
| 찢어진 꼬리는 잘리고 앞은 보존된다 | `a_torn_tail_is_cut_off_and_everything_before_it_survives` | 반쪽 프레임을 덧붙이고 다시 연다 |
| 저널은 두 번 열리지 않고 순서 밖 쓰기를 거부한다 | `a_journal_cannot_be_opened_twice_or_written_out_of_turn` | 두 번째 open은 `WouldBlock`. 건너뛰기·덮어쓰기 배치는 오류이며 흔적이 없다 |
| 저널 쓰기는 겹치지 않는다 | `journal_writes_never_overlap_however_eagerly_they_are_wanted` | 쓰기마다 3ms 걸리는 저널에 창 0, 압축 임계 64로 설정. 동시 진행 최대값이 1, 사슬 연속, 스냅샷 경로 포함 |
| 저널은 대부분 대체되면 스스로 접는다 | `the_journal_folds_itself_when_it_is_mostly_superseded` | 600개의 대체된 레코드 뒤 압축 발생, 파일이 최대치보다 작아짐, 재시작 뒤 남은 일 보존 |
| 엄격 모드는 디스크에 있는 것만 응답한다 | `strict_mode_acknowledges_only_what_is_on_disk` | 응답 직후 복사한 이미지에 50건 전부 있음 |

### 프레임워크 ([`tests/app.rs`](../crates/ruvent/tests/app.rs))

| 요구 | 테스트 | 관측 방법 |
| --- | --- | --- |
| 리액션 연쇄에 함수 간 호출이 없다 | `a_chain_of_reactions_runs_with_no_handler_naming_the_next` | 함수 셋이 사실만 기록. 워커 전용 사실은 클라이언트에 안 보임. 델타 순서 보존. 미등록 action 거부 |
| panic은 재시도, 치명 실패는 기록 | `a_panic_is_retried_and_a_fatal_failure_is_recorded` | 실행 횟수 2, `.failure` 이벤트 |
| 기한을 넘긴 스텝은 버려지고 다시 실행된다 | `a_step_that_overruns_is_dropped_and_retried` | 핸들러가 소유한 값의 `Drop`이 1회 실행. 경과 시간이 정확히 기한 30초 + 백오프 2초 |
| 유휴 서비스는 엔진을 깨우지 않는다 | `idle_services_do_not_wake_the_engine` | 서비스 둘이 떠 있는 채 6시간 동안 `wakeups` 불변 |

### 테스트가 다루지 않는 것

* 실제 전원 차단. 찢어진 꼬리와 "크래시 시점 이미지"는 파일 조작으로 흉내 낸 것이다.
* 저널이 다른 장치(네트워크 디스크 등)에 있을 때의 fsync 의미.
* 장시간 부하에서의 메모리 추이.
* 전송 플러그인 (아직 없다).

## 실측

원자료: [reports/bench-2026-11-22.txt](reports/bench-2026-11-22.txt). 환경과 설정은 그 파일
머리에 있다 (Threadripper 3960X, 논리 코어 48, Windows, rustc 1.96.0, 릴리스 빌드,
`EngineConfig::default()`, 페이로드 256바이트, 동시 256).

### 왕복 — 명령 1건을 손으로 처리

`submit → dequeue → begin → finish`. `per_sec`는 이 왕복이 메모리에 적용되어 응답까지
끝난 건수를 경과 시간으로 나눈 값이다. 핸들러도 워커 런타임도 네트워크도 들어 있지 않다.

| 모드 | 왕복 건수 | 왕복/초 | 저널 레코드 | 저널 쓰기 |
| --- | --- | --- | --- | --- |
| 저널 없음 | 199,936 | 149,215 | (5,939 — 버려짐) | 14 |
| 파일, 창 100ms | 199,936 | 153,847 | 6,531 | 14 |
| 파일, 엄격 | 19,968 | 39,947 | 79,873 | 537 |

### 연쇄 — 프레임워크가 처리

클라이언트 명령 하나를 서비스 A가 받고, A의 사실에 B가, B의 사실에 C가 반응한다.
제출 1건당 실행 3회, 이벤트 9개(1,799,424 / 199,936)다. `per_sec`는 제출부터 마지막
사실이 구독자에게 보일 때까지 끝난 **연쇄** 수다. 실제 워커 런타임을 거친다.

| 모드 | 연쇄 건수 | 연쇄/초 | 이벤트 | 저널 레코드 | 저널 쓰기 |
| --- | --- | --- | --- | --- | --- |
| 저널 없음 | 199,936 | 30,387 | 1,799,424 | (433,741 — 버려짐) | 63 |
| 파일, 창 100ms | 199,936 | 30,485 | 1,799,424 | 432,541 | 63 |
| 파일, 엄격 | 19,968 | 11,774 | 179,712 | 239,617 | 1,263 |

### 읽는 법

* 저널(창 100ms)은 처리량을 깎지 않는다. 두 측정 모두 저널 없는 실행과 같은 수준이다.
* 왕복 측정에서 20만 건에 쓴 레코드는 6,531개다. 대부분의 일이 한 창 안에서 왔다 갔기 때문이다.
  같은 측정을 연산마다 레코드를 적던 이전 구조로 돌렸을 때는 1,399,553개였다.
* 연쇄 측정의 레코드(432,541)가 많은 것은 256개 태스크가 처리보다 빠르게 제출해 큐에 일감이
  쌓인 채 창을 넘겼기 때문이다. "쓸 때 아직 빚진 일"은 적는다. 규칙대로다.
* 엄격 모드에서 왕복당 레코드가 4개(79,873 / 19,968)인 것은 매 배치가 그 순간의 상태를 적기 때문이다.
* `engine_wakeups`는 원자료에 있다. 왕복 측정에서 연산 약 80만 건을 5,966회 깨어나 처리했다.

### 비교하지 않은 것

원본 TypeScript 구현을 같은 조건으로 측정하지 않았다. 원본이 명령 1건에 PostgreSQL
왕복 약 19회를 쓴다는 것은 코드에서 센 값이고([origin.md](origin.md)), 그로부터
"몇 배 빠르다"를 계산하지 않는다.
