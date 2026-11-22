# 테스트

테스트망은 한 번 촘촘히 짜 두고, 이후의 모든 변경을 이 망에 통과시키는 것을 전제로 한다.

## 실행

```sh
cargo xtask verify            # 모든 관문. 변경은 이것이 통과해야 끝난다
cargo xtask verify --deep     # 위 + 속성 테스트를 사례 10만 개로
cargo xtask mutants           # 코어 핵심 파일의 변이 테스트 (cargo-mutants 필요)
cargo run --release -p ruvent --example bench   # 측정
```

마지막 실행 기록: [reports/test-2026-11-23.txt](reports/test-2026-11-23.txt). 측정:
[reports/bench-2026-11-23.txt](reports/bench-2026-11-23.txt).

`verify`는 아래 순서로 돌고, 처음 실패한 관문에서 멈춘다. 관문마다 `gate name=… status=ok|failed` 한 줄을 남긴다.

| 관문 | 명령 | 막는 것 |
| --- | --- | --- |
| format | `cargo fmt --check` | 포맷 차이 |
| lint | `cargo clippy --workspace --all-targets -D warnings` | 경고 전부. 워크스페이스 린트(`unsafe` 금지, await를 넘는 잠금, 라이브러리의 터미널 출력 등) 포함 |
| api docs | `cargo doc --workspace` + `RUSTDOCFLAGS=-D warnings` | 깨진 문서 링크 |
| static | `-p ruvent --test static` | 아래 정적 수준 |
| state machine | `-p ruvent --lib` | 엔벨롭·저널·이름표·상태 기계 단위와 속성 테스트 |
| runtime | `-p ruvent --test runtime` | 엔진 태스크 |
| acceptance | `-p ruvent --test acceptance` | 사용자 원문 인수 |
| protocol | `-p ruvent-protocol` | 와이어 형식 |
| server | `-p ruvent-server` | 실제 TCP 서버 |
| client and workers | `-p ruvent-client` | 워커 프레임워크 |
| proxies | `-p ruvent-proxy -p ruvent-proxy-ws -p ruvent-proxy-http` | 집약·인증·바이너리 라우터 |

## 수준

| 수준 | 묻는 것 | 판정 근거 | 위치 |
| --- | --- | --- | --- |
| 정적 | 실행하지 않고 소스만으로 정해지는 구조 결정이 지켜지는가 | 구조 규칙 | `crates/ruvent/tests/static/` |
| 단위·런타임 | 실제로 돌렸을 때 맞게 동작하는가 | 참조 모델, 고정 바이트, 재생 동치, 실제 TCP | `src/**/tests.rs`, 각 크레이트 `tests/` |
| 인수 | 요구가 충족되는가 | 제안서 2장의 **사용자 원문**. 테스트마다 인용한다 | `crates/ruvent/tests/acceptance/` |

같은 변경에서 쓴 테스트로 그 변경을 증명하지 않도록, 인수 테스트의 판정 기준은 사용자가 준 문장에 둔다.
정적 검사 `every_test_is_on_the_map_and_the_map_names_only_tests`는 워크스페이스의 모든 테스트가 이 문서에
있고, 이 문서에 있는 이름이 모두 실제 테스트인지 확인한다.

## 정적 수준 (`crates/ruvent/tests/static/`)

| 결정 | 테스트 |
| --- | --- |
| 코어 모듈은 아래로만 의존한다(`names` ← `envelope` ← `journal`·`snapshot` ← `engine` ← `methods`) | `modules_depend_only_downward`, `every_module_is_in_the_layer_table_and_no_layer_is_stale` |
| 어느 라이브러리도 폴링하지 않고, 타이머는 선언된 다섯 곳뿐이다(스냅샷 단위 나이, 핸들러 lease, 핸드셰이크 기한, 인증 응답 기한, 상위 재접속 백오프) | `nothing_polls_and_every_timer_is_declared` |
| 저널은 기록 태스크 한 곳에서만, 한 번에 한 비행만 쓴다 | `the_journal_is_written_from_one_place` |
| 엔진 상태의 소유자는 엔진 태스크 하나다 | `the_engine_state_has_one_owner` |
| 라이브러리에 `unsafe`·터미널 출력·`todo!`가 없다 | `no_library_has_unsafe_or_writes_to_the_terminal` |
| 코어는 JSON을 모른다(payload를 해석하지 않는다) | `the_core_knows_no_json` |
| 코어의 벽시계는 스냅샷 파일 이름 한 곳뿐이다 | `the_wall_clock_is_read_once_in_the_core` |
| 서버에 이벤트 정책(시도 상한, dead letter, 리액션 표, 트랜잭션 키, 보존)이 없다 | `the_server_keeps_no_event_policy` |
| 코어 의존성은 선언된 목록이고 모든 크레이트가 워크스페이스 린트를 쓴다 | `runtime_dependencies_are_the_declared_set` |
| 소스와 문서는 제어 문자 없는 UTF-8이다 | `source_files_are_plain_utf8_text`, `documents_are_plain_utf8_text` |
| 모든 크레이트에 README가 있고 루트 README가 링크한다 | `every_crate_is_documented_and_linked` |
| 이 지도는 완전하고, 인수 테스트는 원문을 인용하며, 문서 링크는 살아 있다 | `every_test_is_on_the_map_and_the_map_names_only_tests`, `every_acceptance_test_quotes_its_requirement`, `document_links_resolve` |
| 주석 제거기는 코드와 문자열을 보존한다 | `the_comment_stripper_keeps_code_and_strings`, `the_comment_stripper_is_used_on_code` |

## 보장 ↔ 테스트 (제안서 5장, 8장 합격 조건)

| 보장 | 테스트 |
| --- | --- |
| G1 정확히 하나의 착수 | `of_two_claims_on_one_waiting_event_exactly_one_wins`, `of_many_workers_racing_for_one_event_exactly_one_takes_it` |
| G2 표지·자격이 맞는 완료만 반영, 재전송은 무효·후속 0건 | `completion_from_another_credential_is_refused_and_the_same_credential_on_a_new_connection_succeeds`, `reclaim_keeps_the_work_key_and_a_new_registration_under_the_old_id_gets_another`, `a_resent_completion_after_a_lost_reply_has_no_effect_and_no_follow_ups`, `after_a_server_restart_a_surviving_workers_old_completion_is_void` |
| G3 작업 키는 등록 건마다, 재실행에도 유지 | `reclaim_keeps_the_work_key_and_a_new_registration_under_the_old_id_gets_another`, `recovery_restores_order_and_reissues_what_was_claimed_with_one_more_retry` |
| G4 재시작 복구, Strict 공개 경계 | `a_restart_reloads_the_queue_in_order_and_reissues_claimed_events`, `under_strict_an_answered_registration_survives_a_crash`, `unprocessed_events_come_back_after_a_normal_stop_and_after_a_crash`, `under_strict_registration_waits_for_the_disk_and_a_claim_does_not`, `a_failed_write_stops_the_engine_and_releases_nothing_it_carried`, `a_full_snapshot_replays_to_the_same_queue_as_the_records_it_replaced`, `the_journal_is_compacted_and_the_compacted_file_restarts` |
| G5 발송은 등록 순간의 구독자에게, 포화는 연결 종료 | `dispatch_reaches_exactly_the_subscribers_at_the_moment_it_is_registered`, `subscriptions_take_effect_in_processing_order_within_a_batch`, `a_dispatch_event_goes_to_whoever_is_connected_at_that_moment_and_is_gone` |
| G6 한 접속이 멈춰도 다른 접속은 진행 | `a_connection_that_does_not_read_is_cut_off_and_the_others_carry_on`, `a_slow_user_is_disconnected_and_nobody_else_is` |
| G7 삭제는 대기 중일 때만, 결과 통지는 정확히 한 건 | `deleting_a_waiting_event_succeeds_once_and_a_claimed_or_missing_one_fails_with_one_notice`, `delete_then_claim_and_claim_then_delete`, `the_server_offers_exactly_seven_methods_and_deletion_always_announces_its_outcome` |
| G8 착수용 큐의 id로는 아무것도 등록되지 않는다 | `an_id_in_the_claimable_queue_refuses_every_kind_of_registration`, `reissued_ids_skip_the_old_id_and_every_live_id`, `an_id_in_the_queue_cannot_be_registered_again_ids_are_uuidv7_and_a_departed_id_may_return` |
| Q10 회수 전 완료는 수락, 회수 뒤는 무효 | `a_completion_that_arrives_before_the_reclaim_is_accepted_and_after_it_is_refused` |
| 순서: Strict에서 한 접속은 처리 순서대로 받는다 | `under_strict_one_connection_receives_in_processing_order` |

## 코어 단위 (`crates/ruvent/src/**/tests.rs`)

| 영역 | 테스트 |
| --- | --- |
| 엔벨롭 크기·바이트 | `the_three_sizes_are_31_39_and_63_bytes_plus_payload`, `a_claim_response_has_these_exact_bytes`, `a_dispatch_delivery_has_these_exact_bytes` |
| 레코드 종류별 규칙 | `layouts_refuse_what_they_must_not_carry`, `the_decoder_refuses_forbidden_combinations_reserved_bits_and_leftovers`, `every_envelope_round_trips_through_every_layout_that_accepts_it`, `arbitrary_bytes_never_panic_the_decoder` |
| 재시도 포화, id | `retry_saturates_at_65535`, `ids_are_uuids_and_nil_means_unassigned` |
| 이름표 | `equal_texts_share_one_handle`, `one_off_names_do_not_grow_the_dictionary_without_bound`, `a_name_held_elsewhere_survives_a_sweep`, `one_off_reply_channels_leave_nothing_behind` |
| 저널 v2 | `every_record_kind_survives_a_reopen`, `a_second_engine_cannot_open_a_journal_in_use`, `a_version_1_journal_is_refused_and_left_byte_for_byte`, `a_torn_last_frame_is_cut_and_everything_before_it_recovered`, `a_completion_frame_cut_at_any_byte_recovers_all_or_nothing`, `a_rewritten_journal_decodes_on_its_own_and_accepts_appends` |
| 운영 스냅샷 | `a_snapshot_file_holds_both_kinds_and_reads_back`, `the_configuration_picks_channels_and_types`, `chosen_channels_are_collected_into_snapshot_files` |
| 착수 블록·기한 | `a_claim_answer_carries_the_whole_envelope_with_key_and_claim_block`, `a_thirty_second_claim_expires_after_exactly_thirty_seconds_across_the_u32_millisecond_boundary`, `a_claim_answer_keeps_the_values_it_had_when_it_was_made` |
| 회수 | `only_an_external_registration_reclaims_and_only_once_per_batch`, `a_missed_deadline_is_reclaimed_by_the_next_registration` |
| 세대 | `exhausted_sequences_advance_the_generation_and_gate_the_batch` |
| 구독·대기분 | `the_backlog_comes_in_pages_in_work_key_order`, `the_subscription_marker_precedes_the_answer` |
| 권한·형식 | `grants_limit_methods_and_channels_and_delegation`, `requests_carrying_server_fields_or_the_wrong_kind_are_malformed` |
| 참조 모델 대조(속성) | `the_state_machine_agrees_with_the_reference_model` |
| 엔진 태스크 | `the_seven_methods_work_through_the_in_process_client`, `after_shutdown_calls_fail_as_closed` |

## 인수 (`crates/ruvent/tests/acceptance/requirements.rs`)

테스트 위의 `///` 인용이 판정 기준이다.

| 원문 | 테스트 |
| --- | --- |
| 이벤트 서버의 메서드 1~7 | `the_server_offers_exactly_seven_methods_and_deletion_always_announces_its_outcome` |
| 라이프사이클 2·3 (착수 마킹, 완료 보고) | `a_claim_records_the_worker_and_its_deadline_in_the_event_and_completion_removes_it` |
| 라이프사이클 4, 착수 기한(스케줄러 없음) | `a_timed_out_claim_is_returned_to_waiting_at_the_next_registration_with_a_new_id_and_the_late_report_is_ignored` |
| 라이프사이클 5·6 (실패는 완료, 정책은 워커) | `a_failure_is_a_completion_and_what_follows_is_new_events_the_worker_chose` |
| 이벤트 id | `an_id_in_the_queue_cannot_be_registered_again_ids_are_uuidv7_and_a_departed_id_may_return` |
| 발송 전용 이벤트 | `a_dispatch_event_goes_to_whoever_is_connected_at_that_moment_and_is_gone` |
| 저널링 | `unprocessed_events_come_back_after_a_normal_stop_and_after_a_crash` |
| 이벤트 형식(엔벨롭) | `one_small_envelope_carries_id_channel_retry_and_the_claim_for_both_kinds` |
| 하나의 워커 원칙 | `of_many_workers_racing_for_one_event_exactly_one_takes_it` |
| 역할과 배치(접속자 구분 없음) | `every_connection_gets_the_same_methods_and_the_server_never_asks_what_it_is` |
| 구현 태도·고가용성(착수는 디스크를 기다리지 않음) | `a_claim_does_not_wait_for_the_disk_even_under_strict` |

## 프로토콜 (`crates/ruvent-protocol/src/tests.rs`)

| 영역 | 테스트 |
| --- | --- |
| 고정 바이트 | `a_hello_frame_has_these_exact_bytes`, `a_claim_call_has_these_exact_bytes`, `a_delivery_defines_its_names_first` |
| 왕복 | `every_method_and_every_reply_round_trips`, `calls_round_trip_through_small_dictionaries` |
| 거절 | `a_record_from_the_wrong_direction_refuses_the_frame`, `unknown_tags_leftover_bytes_and_forbidden_bodies_refuse_the_frame`, `arbitrary_bytes_never_panic_the_decoder` |
| 프레임 상한 | `a_frame_over_the_bound_is_refused_on_both_ends`, `messages_are_split_across_frames_under_the_bound` |
| 접속별 사전 | `after_a_reset_an_old_number_is_never_read_as_a_new_name`, `the_receiver_holds_the_same_bound` |

## 서버 (`crates/ruvent-server/`)

| 영역 | 테스트 |
| --- | --- |
| 설정 | `the_example_configuration_parses_into_grants`, `bad_configurations_are_refused_with_a_reason` |
| TCP 위의 메서드 | `all_seven_methods_work_over_tcp`, `local_and_remote_connections_see_the_same_results_in_the_same_order` |
| 자격 | `a_wrong_token_is_refused_with_its_reason`, `a_credential_may_use_only_its_granted_methods_and_channels` |
| 대기분 이어받기 | `a_backlog_larger_than_one_page_is_read_to_the_end_by_subscribing_again` |
| 원격 워커 단절·회수 | `a_claim_held_by_a_disconnected_remote_worker_is_reclaimed_by_the_next_registration` |
| 서버 종료 | `when_the_server_goes_away_the_inbox_closes_and_calls_fail_closed` |

## 워커 프레임워크 (`crates/ruvent-client/tests/worker.rs`)

| 영역 | 테스트 |
| --- | --- |
| 처리와 후속 | `a_handled_event_is_completed_and_its_follow_ups_registered`, `a_listener_receives_dispatch_events_in_order` |
| 실패는 완료, 보고는 선택 | `a_failed_handler_completes_the_event_and_reports_only_when_asked`, `a_panicking_handler_is_a_failure_not_a_dead_worker` |
| 재시도 정책은 워커 쪽 | `an_event_past_its_retry_limit_is_completed_without_running_the_handler`, `a_handler_that_outlives_its_lease_is_reclaimed_and_run_again_with_retry_plus_one` |
| 허가와 보관 알림(P3) | `a_worker_with_every_permit_taken_still_runs_work_noticed_meanwhile_once_a_permit_returns`, `work_reclaimed_ahead_of_the_read_position_and_dropped_by_the_local_limit_is_still_run` |
| 별도 예산 | `a_saturated_executor_does_not_slow_the_event_workers` |
| 장애 표면화(D8) | `the_app_ends_with_an_error_when_the_engine_stops`, `a_refused_subscription_fails_start` |

## 프록시 (`crates/ruvent-proxy*/`)

| 영역 | 테스트 |
| --- | --- |
| 설정·메시지 | `the_example_configuration_parses`, `worker_mode_without_its_section_is_refused`, `a_publish_request_parses_with_its_raw_payload`, `an_event_carries_json_payloads_as_json_and_others_as_strings`, `channel_values_are_collected_and_unescaped` |
| 구독 집약 | `three_users_on_one_channel_cost_one_upstream_delivery`, `a_late_joiner_gets_only_events_after_it_joined`, `resubscribing_never_shows_events_from_the_gap`, `the_last_user_leaving_cancels_the_upstream_subscription`, `after_the_upstream_drops_the_proxy_resubscribes` |
| 사용자 범위·위임 | `a_publish_outside_the_users_channels_is_refused`, `a_registration_carries_the_end_user_as_delegate`, `requests_are_answered_in_the_users_stream` |
| 인증 | `static_tokens_let_in_listed_users_only`, `an_authentication_worker_lets_in_and_turns_away`, `an_authentication_nobody_answers_times_out` |
| WS 바이너리(실제 TCP) | `a_websocket_user_subscribes_receives_and_publishes`, `a_websocket_with_a_wrong_token_is_turned_away`, `health_is_ok_while_the_upstream_is_up` |
| HTTP 바이너리(실제 TCP) | `a_post_reaches_an_sse_subscriber`, `a_post_is_refused_for_a_bad_token_or_a_foreign_channel` |
