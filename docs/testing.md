# 테스트

테스트망은 한 번 촘촘히 짜 두고, 이후의 모든 변경을 이 망에 통과시키는 것을 전제로 한다.

## 실행

```sh
cargo xtask verify            # 모든 관문. 변경은 이것이 통과해야 끝난다
cargo xtask verify --deep     # 위 + 속성 테스트를 사례 10만 개로
cargo xtask mutants           # 코어 핵심 파일의 변이 테스트 (cargo-mutants 필요)
cargo run --release -p ruvent --example bench   # 측정
```

마지막 실행 기록: [reports/test-2026-11-23-proxy-review.txt](reports/test-2026-11-23-proxy-review.txt) (프록시 리뷰 지적 반영과 구조 분리 뒤).
그 전 기록: [reports/test-2026-11-23-proxy.txt](reports/test-2026-11-23-proxy.txt) (프록시를 라이브러리 하나로 합친 뒤),
[reports/test-2026-11-23.txt](reports/test-2026-11-23.txt). 측정:
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
| proxy | `-p ruvent-proxy` | 프록시 단위(장부·메시지·핸드셰이크·설정·인증·규칙·상태 코드·조립기), 허브, 두 프로토콜, 훅, 실제 이벤트 서버에 붙인 인수 |
| proxy: ws only / http only / no protocol | `clippy -p ruvent-proxy --no-default-features [--features ws\|http] --all-targets -D warnings` | 한 프로토콜만, 또는 프로토콜 없이 빌드할 때 깨지는 cfg |

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
| 어느 라이브러리도 폴링하지 않고, 타이머는 선언된 여섯 곳뿐이다(스냅샷 단위 나이, 엔진의 기한 휠, 핸들러 실행 한도, 핸드셰이크 기한, 인증 응답 기한, 상위 재접속 백오프) | `nothing_polls_and_every_timer_is_declared` |
| 저널은 기록 태스크 한 곳에서만, 한 번에 한 비행만 쓴다 | `the_journal_is_written_from_one_place` |
| 엔진 상태의 소유자는 엔진 태스크 하나다 | `the_engine_state_has_one_owner` |
| 라이브러리에 `unsafe`·터미널 출력·`todo!`가 없다 | `no_library_has_unsafe_or_writes_to_the_terminal` |
| 코어는 JSON을 모른다(payload를 해석하지 않는다) | `the_core_knows_no_json` |
| 코어의 벽시계는 스냅샷 파일 이름 한 곳뿐이다 | `the_wall_clock_is_read_once_in_the_core` |
| 서버에 이벤트 정책(시도 상한, dead letter, 리액션 표, 트랜잭션 키, 보존)이 없다. 예외는 격리 문턱(`isolate_after`) 하나 | `the_server_keeps_no_event_policy` |
| 코어 의존성은 선언된 목록이고 모든 크레이트가 워크스페이스 린트를 쓴다 | `runtime_dependencies_are_the_declared_set` |
| 소스와 문서는 제어 문자 없는 UTF-8이다 | `source_files_are_plain_utf8_text`, `documents_are_plain_utf8_text` |
| 모든 크레이트에 README가 있고 루트 README가 링크한다 | `every_crate_is_documented_and_linked` |
| 이 지도는 완전하고, 인수 테스트는 원문을 인용하며, 문서 링크는 살아 있다 | `every_test_is_on_the_map_and_the_map_names_only_tests`, `every_acceptance_test_quotes_its_requirement`, `document_links_resolve` |
| 프록시의 역할 분리: 구독 장부는 순수 상태, 훅의 상위 핸들에는 구독이 없음, `deliver`는 한 곳에서 패닉 방어 뒤에 불림 | `the_proxy_keeps_its_roles_apart` |
| 주석 제거기는 코드와 문자열을 보존한다 | `the_comment_stripper_keeps_code_and_strings`, `the_comment_stripper_is_used_on_code` |

## 보장 ↔ 테스트 (개편 계획 v4.6, `docs/proposals/library-overhaul.md`)

| 보장 | 테스트 |
| --- | --- |
| 정확히 하나의 착수, 배정·착수·배달은 한 스텝 | `of_many_workers_racing_for_one_event_exactly_one_takes_it`, `an_assignment_carries_the_whole_event_its_token_and_the_lease_from_now`, `an_open_window_takes_work_as_it_arrives_without_a_request_per_event` |
| 논리 id는 회수해도 그대로, 시도는 표지로 가른다(2.1) | `a_lease_that_runs_out_reclaims_under_the_same_id_and_closes_the_window`, `registration_claim_and_completion_are_idempotent_and_a_reclaimed_event_keeps_its_id`, `a_lease_end_wakes_the_engine_without_any_registration` |
| 순번은 u64, 세대는 기동 때만 오른다(2.1, B20) | `work_keys_and_tokens_are_u64_past_the_u32_range`, `a_restart_restores_claims_with_token_lease_and_credential_and_counts_no_failure` |
| 생산자 순번: 재전송은 버리고 빈자리는 닫는다(2.3) | `a_retransmitted_sequence_is_dropped_and_a_gap_closes_the_connection`, `the_producer_table_survives_a_restart_and_resume_is_the_last_journaled_sequence`, `an_acknowledged_sequence_survives_a_crash_and_resume_says_so` |
| 누적 확인: 연산별 기준, 커밋 완료와 묶음 끝이 계기, N건(S2), 끌 수 있다(2.3, B2) | `committed_waits_for_the_disk_for_durable_work_and_not_for_volatile`, `committed_goes_out_once_every_n_while_later_work_still_waits`, `a_volatile_broadcast_producer_can_turn_acknowledgements_off`, `registrations_are_not_answered_one_by_one_but_acknowledged_cumulatively`, `welcome_resumes_at_the_last_applied_sequence_after_it_is_committed` |
| 인스턴스 nonce와 `superseded`(2.3, M8-7) | `the_same_process_reconnecting_replaces_quietly_and_another_instance_supersedes`, `a_second_instance_under_one_producer_id_supersedes_the_first_and_both_are_told` |
| 다이제스트로 중복·충돌(2.3) | `a_duplicate_id_with_the_same_content_is_dropped_and_other_content_is_a_conflict_told_once`, `a_follow_up_with_an_active_id_is_skipped_when_identical_and_refuses_the_completion_when_not`, `an_active_id_is_not_registered_twice_ids_are_uuidv7_and_a_departed_id_may_return` |
| 오류 통보는 (연결, 종류)마다 첫 1회, 권한은 앞에서(2.7) | `malformed_requests_invalid_ids_and_forbidden_channels_are_faults`, `declared_channels_are_checked_before_welcome`, `a_call_the_role_does_not_send_is_refused`, `a_join_is_refused_without_the_claim_grant`, `a_completion_under_another_credential_is_a_fault`, `stats_and_drain_need_the_operator_grant` |
| 창은 절댓값, 등록순 순환, `more`·`Pending`, 바이트 진행 규칙(2.4) | `a_window_receives_assignments_round_robin_in_join_order`, `a_full_window_gets_more_on_the_last_assignment_and_pending_once_per_window`, `a_window_of_zero_refuses_and_a_join_holding_nothing_takes_one_event_over_its_byte_bound`, `types_are_lanes_and_an_unsupported_type_does_not_block_the_rest` |
| lease는 배정 시점부터, 회수 때 창을 닫는다, 회수 전 완료는 수락(2.4) | `a_lease_that_runs_out_reclaims_under_the_same_id_and_closes_the_window`, `a_late_completion_before_the_reclaim_is_accepted_and_after_it_is_dropped` |
| 연결이 닫히면: 못 쓴 배정은 즉시 대기로, 다 쓴 배정은 재접속 유예로(2.4, S20, M8-2) | `a_closed_connection_returns_unwritten_assignments_at_once_and_pulls_written_ones_to_the_grace`, `a_closing_connection_gives_back_unwritten_assignments_at_once` |
| `Extend`는 참여를 옮긴다, 재기동은 착수를 그대로 둔다(2.4, M8-6) | `extend_moves_the_attempt_to_the_extending_connections_join`, `a_restart_restores_claims_with_token_lease_and_credential_and_counts_no_failure`, `a_restart_keeps_waiting_order_and_claims_and_accepts_the_original_workers_completion` |
| `Release{지연, 실패}`와 격리(이벤트 탓만 센다, 응답 채널 제외)(2.4, B6, M8-12) | `release_keeps_or_raises_the_failure_count_and_a_delay_hides_the_event`, `the_fifth_counted_failure_isolates_and_releases_without_failure_never_do`, `reply_channels_are_never_isolated` |
| `not_before`·`expires`(깨우지 않는 수거, 재기동 뒤 남은 시간)(2.4, B9, M8-11) | `not_before_hides_an_event_until_its_time_and_the_wheel_wakes_for_it`, `an_expired_event_is_never_assigned_and_is_purged_without_any_consumer`, `an_expired_event_is_isolated_instead_where_the_channel_says_so`, `a_restart_keeps_the_remaining_time_of_not_before_and_expires` |
| `serialize_group`(B26) | `serialize_group_assigns_one_event_of_a_group_at_a_time` |
| 명시 착수 | `an_explicit_claim_takes_a_waiting_event_and_refuses_claimed_delayed_or_missing_ones`, `a_claim_carries_the_attempt_and_the_time_out_the_worker_chose_and_completion_removes_it` |
| 완료와 후속은 한 스텝, 후속은 부모의 뿌리(0.3, 2.1) | `a_completion_registers_its_follow_ups_in_the_same_step_under_the_parents_root`, `a_completion_with_more_follow_ups_than_allowed_is_refused`, `a_failure_is_a_completion_and_what_follows_is_new_events_the_worker_chose` |
| Strict 커밋 가시성: 배정·통지·backlog·명시 착수(2.7, M8-3) | `under_strict_nothing_uncommitted_is_assigned_noticed_backlogged_or_claimed`, `under_strict_nothing_uncommitted_is_visible_while_the_disk_is_held`, `under_scheduled_visibility_does_not_wait_for_the_disk`, `under_strict_one_connection_receives_in_processing_order`, `a_claim_does_not_wait_for_the_disk_even_under_strict` |
| 메모리 회계, 고수위, 뿌리 채널 정지와 해제, 통보와 읽기 정지, 조기 정지, `max_backlog`, 순증 줄, 상한(2.6) | `the_biggest_root_is_paused_at_the_high_watermark_and_resumed_below_seven_eighths`, `a_paused_root_tells_its_producers_once_and_stops_reading_past_the_allowance`, `a_paused_root_is_not_assigned_from_net_growth_lanes_and_chain_ends_still_are`, `a_root_that_never_drained_is_paused_at_an_eighth_of_the_budget`, `max_backlog_pauses_a_root_regardless_of_the_watermark`, `follow_ups_and_isolated_events_count_to_the_root_channel`, `past_the_limit_net_growth_lanes_are_not_assigned`, `a_full_server_refuses_nothing_and_tells_the_producer_of_the_full_channel_to_hold` |
| 발송은 등록 순간의 구독자에게, 접두사는 `.` 경계, 발송 포함 참여(2.4, 2.8, 4장) | `dispatch_reaches_exactly_the_subscribers_at_the_moment_it_is_registered`, `a_prefix_subscription_matches_at_dot_boundaries_unlike_a_grant_pattern`, `a_dispatch_join_receives_the_channels_dispatch_events`, `subscriptions_take_effect_in_processing_order_within_a_batch`, `a_dispatch_event_goes_to_whoever_is_connected_at_that_moment_and_is_gone` |
| 한 접속이 멈춰도 다른 접속은 진행 | `a_connection_that_does_not_read_is_cut_off_and_the_others_carry_on`, `a_slow_user_is_disconnected_and_nobody_else_is` |
| 삭제는 대기 중일 때만, 결과 통지는 정확히 한 건 | `deleting_a_waiting_event_succeeds_once_and_a_claimed_or_missing_one_fails_with_one_notice`, `deletion_removes_only_a_waiting_event_and_always_announces_its_outcome` |
| `Stats` 상위 K, `Drain`(2.8, B24, M8-5) | `stats_report_totals_top_channels_idle_lanes_and_roots`, `drain_sends_new_producers_away_and_closes_open_ones_after_a_final_committed`, `drain_gives_every_producer_a_final_committed_before_bye` |
| 재기동·저널: 휘발 채널 결과표, snapshot 재생 동치, 정상 종료 snapshot, 누적 가동 시각 | `volatile_channels_write_nothing_and_mixed_lineages_restart_as_the_table_says`, `a_full_snapshot_replays_to_the_same_ledger_as_the_records_it_replaced`, `a_shutdown_writes_a_snapshot_and_the_uptime_clock_carries_on`, `unprocessed_events_come_back_after_a_normal_stop_and_after_a_crash`, `a_failed_write_stops_the_engine_and_releases_nothing_it_carried`, `the_journal_is_compacted_and_the_compacted_file_restarts` |
| 장부 정합성(속성): 한 이벤트는 한 자리, 줄·참여·회계·group 계수의 합이 맞다 | `random_operations_keep_the_ledgers_bookkeeping_consistent` |
| 옛 클라이언트 경로(다른 크레이트. 그 크레이트의 에이전트가 새 계약으로 옮긴다) | `a_resent_completion_after_a_lost_reply_has_no_effect_and_no_follow_ups`, `after_a_server_restart_a_surviving_workers_old_completion_is_void` |

## 코어 단위 (`crates/ruvent/src/**/tests.rs`, `src/engine/state/tests/`)

| 영역 | 테스트 |
| --- | --- |
| 엔벨롭 크기·바이트(u16 flags, 가변 길이 정수) | `the_head_is_28_bytes_and_the_key_adds_four_plus_a_varint`, `an_assigned_event_has_these_exact_bytes`, `a_dispatch_delivery_has_these_exact_bytes`, `varints_round_trip_and_refuse_overlong_encodings` |
| 레코드 종류별 규칙 | `layouts_refuse_what_they_must_not_carry`, `timing_and_group_are_claimable_only`, `the_decoder_refuses_forbidden_combinations_reserved_bits_and_leftovers`, `every_envelope_round_trips_through_every_layout_that_accepts_it`, `arbitrary_bytes_never_panic_the_decoder` |
| 실패 횟수 포화, id | `failures_saturate_at_65535`, `ids_are_uuids_and_nil_means_unassigned` |
| 이름표 | `equal_texts_share_one_handle`, `one_off_names_do_not_grow_the_dictionary_without_bound`, `a_name_held_elsewhere_survives_a_sweep` |
| 저널 형식 3(레코드 집합, 단일 파일 컨테이너) | `every_record_kind_survives_a_reopen`, `a_claim_removed_in_the_same_batch_is_not_written`, `a_second_engine_cannot_open_a_journal_in_use`, `a_format_2_or_older_journal_is_refused_and_left_byte_for_byte`, `a_torn_last_frame_is_cut_and_everything_before_it_recovered`, `a_batch_that_breaks_the_serial_chain_ends_replay`, `a_completion_frame_cut_at_any_byte_recovers_all_or_nothing`, `a_rewritten_journal_decodes_on_its_own_and_accepts_appends` |
| 운영 스냅샷 | `a_snapshot_file_holds_both_kinds_and_reads_back`, `the_configuration_picks_channels_and_types`, `chosen_channels_are_collected_into_snapshot_files` |
| 구독·대기분 | `the_backlog_comes_in_pages_in_work_key_order`, `the_subscription_marker_precedes_the_answer` |
| 엔진 태스크 | `the_operations_work_through_the_in_process_client`, `after_shutdown_calls_fail_as_closed` |

## 인수 (`crates/ruvent/tests/acceptance/requirements.rs`)

테스트 위의 `///` 인용이 판정 기준이다. 개편 계획이 바꾼 요구(회수 때 id 재발급, 접속자 구분 없음)는 바뀐 동작이 따르는 원문으로 인용을 바꿨다.

| 원문 | 테스트 |
| --- | --- |
| 메서드 5(삭제와 결과 통지) | `deletion_removes_only_a_waiting_event_and_always_announces_its_outcome` |
| 라이프사이클 2·3 (착수 마킹, 완료 보고) | `a_claim_carries_the_attempt_and_the_time_out_the_worker_chose_and_completion_removes_it` |
| 9.5의 6 (등록·착수·완료의 멱등) | `registration_claim_and_completion_are_idempotent_and_a_reclaimed_event_keeps_its_id` |
| 라이프사이클 5·6 (실패는 완료, 정책은 워커) | `a_failure_is_a_completion_and_what_follows_is_new_events_the_worker_chose` |
| 이벤트 id | `an_active_id_is_not_registered_twice_ids_are_uuidv7_and_a_departed_id_may_return` |
| 발송 전용 이벤트 | `a_dispatch_event_goes_to_whoever_is_connected_at_that_moment_and_is_gone` |
| 저널링 | `unprocessed_events_come_back_after_a_normal_stop_and_after_a_crash` |
| 이벤트 형식(엔벨롭) | `one_small_envelope_carries_id_channel_failures_and_the_attempt_for_both_kinds` |
| 하나의 워커 원칙 | `of_many_workers_racing_for_one_event_exactly_one_takes_it` |
| 역할과 배치(접속자 구분 없음) | `any_program_may_open_any_role_and_the_server_never_asks_what_it_is` |
| 구현 태도·고가용성(착수는 디스크를 기다리지 않음) | `a_claim_does_not_wait_for_the_disk_even_under_strict` |
| 0.2 (들어차고 신속하게 비워진다) | `an_open_window_takes_work_as_it_arrives_without_a_request_per_event` |
| 9.5 (수신완료 반송 없음) | `registrations_are_not_answered_one_by_one_but_acknowledged_cumulatively` |
| 9.5 (거절은 안 보낸다) | `a_full_server_refuses_nothing_and_tells_the_producer_of_the_full_channel_to_hold` |

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

## 프록시 (`crates/ruvent-proxy/`)

범주마다 단위(`src/**/tests.rs`: 순수 함수와 상태, 네트워크 없음) → 통합(`tests/hub.rs`: 허브와 같은 프로세스의 엔진,
`tests/ws.rs`·`tests/http.rs`·`tests/hooks.rs`: 실제 TCP의 프록시) → 인수(`tests/acceptance.rs`: 실제 `ruvent-server`에
TCP로 붙은 `Remote` 상위, 예제 바이너리를 별도 프로세스로 실행) 순으로 쌓는다. 인수 테스트는 판정하는 원문을 `/// >`로
인용한다.

### 단위

| 범주 | 테스트 |
| --- | --- |
| 구독 장부: 가입과 표지 | `a_join_waits_for_the_marker_of_its_own_subscribe`, `every_join_issues_its_own_subscribe`, `an_earlier_marker_does_not_activate_a_later_joiner`, `joining_twice_is_answered_at_once_without_a_call`, `a_marker_on_one_channel_leaves_other_channels_alone` |
| 구독 장부: 요청하지 않은 표지 | `a_marker_nobody_asked_for_activates_nobody`, `a_marker_for_an_unknown_channel_is_ignored`, `a_refusal_nobody_asked_for_is_ignored` |
| 구독 장부: 탈퇴와 `0→1→0→1` | `only_the_last_user_leaving_unsubscribes`, `leaving_before_the_marker_answers_the_waiting_join`, `an_entry_outlives_its_users_until_its_last_marker`, `resubscribing_waits_for_the_second_subscribes_marker`, `rejoining_before_the_first_marker_waits_for_the_second`, `leaving_all_leaves_every_channel`, `leaving_an_unknown_channel_does_nothing` |
| 구독 장부: 거절 | `a_refusal_fails_the_join_it_answers`, `a_refusal_counts_like_a_marker_for_ordering` |
| 구독 장부: 상위 연결과 내부 채널 | `a_join_before_the_upstream_connects_takes_effect_after_it_connects`, `a_new_connection_subscribes_every_wanted_channel_once`, `a_channel_left_while_disconnected_is_not_resubscribed`, `the_internal_channel_is_kept_and_settles_on_its_marker`, `leaving_the_internal_channel_never_unsubscribes` |
| 사용자 JSON 해석 | `a_publish_request_parses_with_its_raw_payload`, `every_op_parses_to_its_request`, `malformed_messages_are_errors` |
| 등록 변환(`Publish::check`, `envelope`, payload 읽기·교체) | `a_publish_becomes_an_event_of_its_kind`, `optional_fields_are_carried_over`, `malformed_ids_are_invalid_requests`, `a_payload_is_read_and_replaced_as_json` |
| 거절 원인과 응답 문자열 | `every_refusal_maps_to_a_cause`, `rejection_constructors_set_their_cause`, `a_result_text_never_shows_the_cause`, `an_event_carries_json_payloads_as_json_and_others_as_strings`, `an_event_shows_its_correlation` |
| 핸드셰이크: 쿼리 | `channel_values_are_collected_in_order_and_decoded`, `encoded_keys_are_the_same_key`, `empty_values_are_kept_for_the_protocol_to_judge`, `the_first_of_a_repeated_key_wins`, `broken_escapes_are_kept_as_text` |
| 핸드셰이크: Bearer·쿠키·헤더 | `a_bearer_token_is_read_whatever_the_schemes_case`, `other_schemes_and_empty_tokens_are_no_bearer_token`, `cookies_are_found_across_headers_and_the_first_wins`, `a_cookie_name_matches_exactly`, `a_header_that_is_not_text_reads_as_absent` |
| 설정 | `the_example_configuration_parses`, `a_minimal_configuration_takes_the_defaults`, `unknown_keys_are_refused`, `worker_mode_without_its_section_is_refused`, `values_that_cannot_work_are_refused`, `a_configuration_built_in_code_is_checked_the_same_way`, `a_missing_file_is_an_error` |
| 사용자와 설정 인증 모드 | `a_users_scope_is_exact_names_and_star_prefixes`, `domain_data_comes_back_only_as_its_type`, `debug_output_hides_the_domain_value`, `denials_read_as_sentences`, `mode_none_lets_everyone_in_as_anonymous`, `mode_static_maps_listed_tokens_to_users`, `mode_static_tells_missing_from_rejected`, `mode_worker_leaves_tokens_to_the_worker`, `a_workers_answer_decides`, `a_workers_answer_reads_with_defaults` |
| 기본 훅 규칙 | `the_subscribe_rule_is_the_users_scope`, `the_publish_rule_checks_the_channel_then_the_reply`, `the_default_deliver_lets_everything_through` |
| 상태 코드와 요청→핸드셰이크 | `every_cause_has_its_status`, `every_denial_has_its_status`, `a_handshake_is_built_from_the_request`, `without_connection_info_the_peer_is_unknown` |
| 서버 조립기 | `the_first_argument_names_the_configuration`, `no_argument_is_a_usage_error`, `a_configuration_given_in_code_is_checked`, `a_bad_configuration_file_is_refused_naming_it`, `a_launch_error_prints_as_its_sentence`, `an_address_that_cannot_be_bound_is_named` |

### 허브 (`tests/hub.rs`)

| 범주 | 테스트 |
| --- | --- |
| 구독 집약과 효력 시점 | `three_users_on_one_channel_cost_one_upstream_delivery`, `a_late_joiner_gets_only_events_after_it_joined`, `resubscribing_never_shows_events_from_the_gap`, `the_last_user_leaving_cancels_the_upstream_subscription`, `after_the_upstream_drops_the_proxy_resubscribes`, `a_subscription_made_before_the_upstream_is_up_takes_effect_when_it_is` |
| 사용자 차단과 정리 | `a_slow_user_is_disconnected_and_nobody_else_is`, `a_user_that_went_away_is_dropped_even_when_the_hook_filters_it_out` |
| 사용자 범위·위임·요청 | `a_publish_outside_the_users_channels_is_refused`, `a_registration_carries_the_end_user_as_delegate`, `requests_are_answered_in_the_users_stream`, `a_publish_is_checked_for_shape_before_the_hook_runs` |
| 설정 인증 | `static_tokens_let_in_listed_users_only`, `an_authentication_worker_lets_in_and_turns_away`, `an_authentication_nobody_answers_times_out` |
| 허브 안의 훅 | `a_deliver_hook_that_panics_costs_that_user_its_stream_and_nobody_else`, `a_hooks_upstream_calls_leave_the_subscription_rule_intact` |
| 허브 수명 | `stopping_the_hub_ends_every_stream_and_lets_go_of_the_upstream`, `dropping_every_handle_stops_the_hub` |

### 프로토콜과 훅 (실제 TCP)

| 범주 | 테스트 |
| --- | --- |
| WS 세션 | `a_websocket_user_subscribes_receives_and_publishes`, `a_malformed_message_is_answered_and_the_session_goes_on`, `mode_none_needs_no_token` |
| WS 토큰 | `a_websocket_with_a_wrong_token_is_turned_away`, `an_empty_query_token_is_a_token_and_is_rejected`, `the_first_of_two_query_tokens_is_the_token`, `a_first_message_that_is_not_auth_ends_the_socket`, `a_wrong_in_band_token_is_rejected_without_asking_again` |
| health와 정지 | `health_is_ok_while_the_upstream_is_up`, `once_the_hub_stopped_health_is_503_and_sockets_close`, `an_sse_stream_ends_when_the_hub_stops` |
| HTTP 등록 | `a_post_reaches_an_sse_subscriber`, `a_post_is_refused_for_a_bad_token_or_a_foreign_channel`, `a_missing_bearer_token_is_401_authentication_required`, `a_body_that_is_not_a_publish_is_400`, `a_malformed_id_is_400_even_when_the_reply_channel_is_forbidden`, `a_duplicate_waiting_id_is_409` |
| HTTP 구독(SSE) | `a_stream_naming_no_channel_is_400`, `an_sse_subscription_outside_the_scope_is_403`, `one_stream_carries_several_channels` |
| `Server`와 설정 파일만 | `a_configuration_file_and_nothing_else_is_a_running_proxy_for_both_protocols` |
| 훅: 인증·사용자 데이터·구독 판단 | `custom_hooks_decide_who_comes_in_and_carry_the_domains_data`, `the_authenticate_hook_is_asked_at_most_twice_per_websocket` |
| 훅: 등록 거절 원인·재작성·재작성 뒤 reply 검사 | `a_publish_hook_refuses_with_its_own_cause_and_rewrites_what_it_allows`, `a_rewriting_publish_hook_still_checks_the_reply_channel` |
| 훅: 사용자별 전달 | `a_deliver_hook_picks_which_users_see_an_event` |

### 인수 (`tests/acceptance.rs`, 실제 `ruvent-server`)

| 판정하는 원문 | 테스트 |
| --- | --- |
| "별거 없이 빌드를 하면 뚝딱하고 프록시서버가 실행가능한 서버로 성립" (예제 `plain`을 별도 프로세스로, argv 설정, `Remote`) | `the_three_line_main_is_a_proxy_server_given_only_a_configuration_file` |
| "라이브러리가 지원하는 프로토콜을 선택해서 빌드" | `every_compiled_in_protocol_answers_on_the_one_listen_address` |
| "스프링 컨트롤러 수준으로 쉽게 훅이 되는 기능만 작성" | `one_hook_method_customizes_a_proxy_against_a_real_event_server` |
| 구독 효력 시점 불변식(허브 머리 주석) | `the_subscription_rule_holds_through_a_real_event_server` |
| `run()`은 Ctrl-C에 끝남(README), 신호 실패는 오류 | `a_shutdown_signal_stops_the_server_and_every_user`, `a_shutdown_signal_that_cannot_be_listened_for_is_an_error` |

인수 테스트의 예제 실행은 `cargo test -p ruvent-proxy`가 함께 빌드한 `target/<profile>/examples/plain`을 쓴다.
`--test acceptance`만 따로 돌리면 예제가 옛 빌드일 수 있다.
