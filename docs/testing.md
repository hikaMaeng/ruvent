# 테스트

테스트망은 한 번 촘촘히 짜 두고, 이후의 모든 변경을 이 망에 통과시키는 것을 전제로 한다.

## 실행

```sh
cargo xtask verify            # 모든 관문. 변경은 이것이 통과해야 끝난다
cargo xtask verify --deep     # 위 + 속성 테스트를 사례 10만 개로
cargo xtask mutants           # 코어 핵심 파일의 변이 테스트 (cargo-mutants 필요)
cargo run --release -p ruvent --example bench   # 측정
```

마지막 실행 기록: [reports/test-2026-11-26-answer-channels.txt](reports/test-2026-11-26-answer-channels.txt) (v4.8: 요청별 응답 채널, 요청자 제거, 엔벨롭에서 `reply`·`correlation` 제거, wire v3·저널 형식 4. 관문 11개 ok, 257 통과·0 실패). 그 전: [reports/test-2026-11-26-skeleton.txt](reports/test-2026-11-26-skeleton.txt) (v4.7).
그 전: [reports/test-2026-11-25-overhaul.txt](reports/test-2026-11-25-overhaul.txt) (개편 계획 v4.6 구현 뒤),
[reports/test-2026-11-23-proxy-review.txt](reports/test-2026-11-23-proxy-review.txt) (프록시 리뷰 지적 반영과 구조 분리 뒤).
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
| client SDK | `-p ruvent-client` | 서버 목록, 재접속, 생산자, 워커 창 규칙 |
| proxy | `-p ruvent-proxy` | 프록시 골조: 구독 장부와 health 단위, 팬아웃(SDK `Local`), 실제 이벤트 서버(서버 2대, 상향 상실, UUIDv7이 아닌 채널)에 붙인 인수 |

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
| 코어 모듈은 아래로만 의존한다(`names` ← `envelope` ← `journal`·`snapshot` ← `engine` ← `methods`). 로그(`journal`)와 엔진(`engine`)의 하위 모듈은 하위 계층 표가 정한 것만 부른다 | `modules_depend_only_downward`, `every_module_is_in_the_layer_table_and_no_layer_is_stale`, `the_parts_of_the_log_and_the_engine_name_only_the_parts_they_may` |
| 어느 라이브러리도 폴링하지 않고, 타이머는 선언된 곳뿐이다(스냅샷 단위 나이, 엔진의 기한 휠, 핸드셰이크 기한, 닫기 기한, SDK의 재접속 백오프·자동 연장·종료 유예·실행 한도. 프록시 골조는 타이머가 없다) | `nothing_polls_and_every_timer_is_declared` |
| 영속 로그는 기록 태스크 한 곳에서만, 한 번에 한 비행만 쓴다. 체크포인트 작성기와 잡일(이주·삭제)도 각각 한 곳이다 | `the_journal_is_written_from_one_place` |
| 엔진 상태의 소유자는 엔진 태스크 하나다 | `the_engine_state_has_one_owner` |
| 라이브러리에 `unsafe`·터미널 출력·`todo!`가 없다 | `no_library_has_unsafe_or_writes_to_the_terminal` |
| 코어는 JSON을 모른다(payload를 해석하지 않는다) | `the_core_knows_no_json` |
| 코어의 벽시계는 스냅샷 파일 이름 한 곳뿐이다 | `the_wall_clock_is_read_once_in_the_core` |
| 서버에 이벤트 정책(시도 상한, dead letter, 격리, 만료, 순서, 정지, 리액션 표, 트랜잭션 키, 보존)이 없다(계획 9.15) | `the_server_keeps_no_event_policy` |
| 코어 의존성은 선언된 목록이고 모든 크레이트가 워크스페이스 린트를 쓴다 | `runtime_dependencies_are_the_declared_set` |
| 소스와 문서는 제어 문자 없는 UTF-8이다 | `source_files_are_plain_utf8_text`, `documents_are_plain_utf8_text` |
| 모든 크레이트에 README가 있고 루트 README가 링크한다 | `every_crate_is_documented_and_linked` |
| 이 지도는 완전하고, 인수 테스트는 원문을 인용하며, 문서 링크는 살아 있다 | `every_test_is_on_the_map_and_the_map_names_only_tests`, `every_acceptance_test_quotes_its_requirement`, `document_links_resolve` |
| 프록시는 골조: 구독 장부는 순수 상태, HTTP·직렬화·생산·요청·훅 없음, 의존은 장부·SDK·런타임뿐(4장) | `the_proxy_is_a_skeleton` |
| 주석 제거기는 코드와 문자열을 보존한다 | `the_comment_stripper_keeps_code_and_strings`, `the_comment_stripper_is_used_on_code` |

## 보장 ↔ 테스트 (개편 계획 v5, `docs/proposals/library-overhaul.md`)

| 보장 | 테스트 |
| --- | --- |
| 정확히 하나의 착수, 배정·착수·배달은 한 스텝 | `of_many_workers_racing_for_one_event_exactly_one_takes_it`, `an_assignment_carries_the_whole_event_and_the_worker_uuid`, `an_open_window_takes_work_as_it_arrives_without_a_request_per_event` |
| 논리 id는 회수해도 그대로, 시도는 표지로 가른다(2.1) | `a_lease_that_runs_out_reclaims_under_the_same_id_and_closes_the_window`, `registration_claim_and_completion_are_idempotent_and_a_reclaimed_event_keeps_its_id`, `a_lease_end_wakes_the_engine_without_any_registration` |
| 순번은 u64, 세대는 기동 때만 오른다(2.1, B20) | `work_keys_are_u64_past_the_u32_range`, `a_restart_restores_claims_with_lease_and_worker_and_counts_no_failure` |
| 생산자 순번: 재전송은 버리고 빈자리는 닫는다(2.3) | `a_retransmitted_sequence_is_dropped_and_a_gap_closes_the_connection`, `the_producer_table_survives_a_restart_and_resume_is_the_last_journaled_sequence`, `an_acknowledged_sequence_survives_a_crash_and_resume_says_so` |
| 누적 확인: 연산별 기준, 커밋 완료와 묶음 끝이 계기, N건(S2), 끌 수 있다(2.3, B2) | `committed_waits_for_the_disk_for_claimable_work_and_not_for_dispatch`, `committed_goes_out_once_every_n_while_later_work_still_waits`, `a_volatile_broadcast_producer_can_turn_acknowledgements_off`, `registrations_are_not_answered_one_by_one_but_acknowledged_cumulatively`, `welcome_resumes_at_the_last_applied_sequence_after_it_is_committed` |
| 인스턴스 nonce와 `superseded`(2.3, M8-7) | `the_same_process_reconnecting_replaces_quietly_and_another_instance_supersedes`, `a_second_instance_under_one_producer_id_supersedes_the_first_and_both_are_told` |
| 다이제스트로 중복·충돌(2.3) | `a_duplicate_id_with_the_same_content_is_dropped_and_other_content_is_a_conflict_told_once`, `a_follow_up_with_an_active_id_is_skipped_when_identical_and_refuses_the_completion_when_not`, `an_active_id_is_not_registered_twice_ids_are_uuidv7_and_a_departed_id_may_return` |
| 오류 통보는 (연결, 종류)마다 첫 1회, 권한은 앞에서(2.7) | `malformed_requests_invalid_ids_and_forbidden_kinds_are_faults`, `a_producer_that_may_register_nothing_is_refused_before_welcome`, `a_call_the_role_does_not_send_is_refused`, `a_join_needs_the_claim_method_and_nothing_narrower`, `a_report_naming_another_worker_is_stale_and_changes_nothing`, `stats_and_drain_need_the_operator_grant` |
| 창은 절댓값, 등록순 순환, `more`·`Pending`, 바이트 진행 규칙(2.4) | `a_window_receives_assignments_round_robin_in_join_order`, `a_full_window_gets_more_on_the_last_assignment_and_pending_once_per_window`, `a_window_of_zero_refuses_and_a_join_holding_nothing_takes_one_event_over_its_byte_bound`, `types_are_lanes_and_an_unsupported_type_does_not_block_the_rest` |
| lease는 배정 시점부터, 회수 때 창을 닫는다, 회수 전 완료는 수락(2.4) | `a_lease_that_runs_out_reclaims_under_the_same_id_and_closes_the_window`, `a_late_completion_before_the_reclaim_is_accepted_and_after_it_is_dropped` |
| 연결이 닫히면: 못 쓴 배정은 즉시 대기로, 다 쓴 배정은 재접속 유예로(2.4, S20, M8-2) | `a_closed_connection_returns_unwritten_assignments_at_once_and_pulls_written_ones_to_the_grace`, `a_closing_connection_gives_back_unwritten_assignments_at_once` |
| `Extend`는 참여를 옮긴다, 재기동은 착수를 그대로 둔다(2.4, M8-6) | `extend_moves_the_attempt_to_the_extending_connections_join`, `a_restart_restores_claims_with_lease_and_worker_and_counts_no_failure`, `a_restart_keeps_waiting_order_and_claims_and_accepts_the_original_workers_completion` |
| `Release{지연, 실패}`와 실패 횟수(이벤트 탓만 센다, 서버는 세기만 한다)(2.4, 9.15, M8-12) | `release_keeps_or_raises_the_failure_count_and_a_delay_hides_the_event`, `failures_keep_counting_and_the_server_never_acts_on_them` |
| `not_before`(재기동 뒤 남은 시간)(2.4, M8-11) | `not_before_hides_an_event_until_its_time_and_the_wheel_wakes_for_it`, `a_restart_keeps_the_remaining_time_of_not_before` |
| 명시 착수 | `an_explicit_claim_takes_a_waiting_event_and_refuses_claimed_delayed_or_missing_ones`, `a_claim_carries_the_attempt_and_the_time_out_the_worker_chose_and_completion_removes_it` |
| 완료와 후속은 한 스텝(0.3, 2.1) | `a_completion_registers_its_follow_ups_in_the_same_step`, `a_completion_with_more_follow_ups_than_allowed_is_refused`, `a_failure_is_a_completion_and_what_follows_is_new_events_the_worker_chose` |
| Strict 커밋 가시성: 배정·통지·backlog·명시 착수(2.7, M8-3) | `under_strict_nothing_uncommitted_is_assigned_noticed_backlogged_or_claimed`, `under_strict_nothing_uncommitted_is_visible_while_the_disk_is_held`, `under_scheduled_visibility_does_not_wait_for_the_disk`, `under_strict_one_connection_receives_in_processing_order`, `a_claim_does_not_wait_for_the_disk_even_under_strict` |
| 메모리 회계와 고수위: 캐시된 본문만 내리고 아무도 멈추지 않는다(2.6, 9.15) | `a_full_server_refuses_nothing_and_holds_up_no_producer` |
| 발송은 등록 순간의 구독자에게, 채널은 UUIDv7만, 발송 포함 참여(2.4, 2.8, 4장, 9.14) | `dispatch_reaches_exactly_the_subscribers_at_the_moment_it_is_registered`, `a_channel_that_is_not_a_uuidv7_is_refused_everywhere`, `a_dispatch_join_receives_the_channels_dispatch_events`, `subscriptions_take_effect_in_processing_order_within_a_batch`, `a_dispatch_event_goes_to_whoever_is_connected_at_that_moment_and_is_gone` |
| 한 접속이 멈춰도 다른 접속은 진행 | `a_connection_that_does_not_read_is_cut_off_and_the_others_carry_on`, `a_slow_member_is_detached_and_nobody_else_is` |
| 프록시 골조: 모든 서버에 서버별 상향, 조용히 끊긴 상향은 준비 상태에 정상으로 남지 않음, 구성원 구독의 효력 시점, UUIDv7이 아닌 채널의 거절(4장, 9.14, M8-8 준비 상태) | `a_fanout_on_two_servers_hands_members_what_either_dispatches`, `an_upstream_that_goes_away_shows_in_health`, `the_subscription_rule_holds_through_a_real_event_server`, `a_channel_that_is_not_a_uuid_v7_is_refused_through_a_real_event_server`, `each_request_is_answered_on_its_own_channel_in_order` |
| 삭제는 대기 중일 때만, 결과 통지는 정확히 한 건 | `deleting_a_waiting_event_succeeds_once_and_a_claimed_or_missing_one_fails_with_one_notice`, `deletion_removes_only_a_waiting_event_and_always_announces_its_outcome` |
| `Stats` 상위 K, `Drain`(2.8, B24, M8-5) | `stats_report_totals_top_types_and_idle_lanes`, `drain_sends_new_producers_away_and_closes_open_ones_after_a_final_committed`, `drain_gives_every_producer_a_final_committed_before_bye` |
| 재기동·저널: 체크포인트 재생 동치, 정상 종료 체크포인트(꼬리 없음), 누적 가동 시각 | `a_full_checkpoint_replays_to_the_same_ledger_as_the_records_it_replaced`, `a_shutdown_writes_a_checkpoint_the_next_start_reads_no_tail_and_the_clock_carries_on`, `unprocessed_events_come_back_after_a_normal_stop_and_after_a_crash`, `a_failed_write_stops_the_engine_and_releases_nothing_it_carried` |
| 퍼지 체크포인트: 훑기 도중의 변경이 재생에서 빠지지 않음, 꼬리의 등록이 위치를 채움, 계기(S4)(2.5, B25) | `a_fuzzy_checkpoint_misses_no_change_made_during_its_walk`, `a_tail_registration_gives_a_checkpoint_entry_its_payload_location`, `a_checkpoint_starts_when_the_tail_passes_one_segment_or_eight_times_the_last` |
| 세그먼트 수명: 제거가 디스크에 닿고 체크포인트가 뒤에서 시작해야 지운다, 체크포인트가 빈 세그먼트를 놓아 준다(2.5) | `a_segment_goes_once_its_removals_land_and_the_checkpoint_starts_after_it`, `checkpoints_let_empty_segments_go_and_the_log_restarts_from_what_is_left` |
| 이주: 붙잡힌 죽은 바이트가 예산 비율(S3)을 넘으면 가장 성긴 세그먼트, `Relocate`가 디스크에 닿은 뒤 놓음(2.5) | `pinned_dead_bytes_relocate_the_sparsest_segment_and_free_it_once_on_disk`, `relocation_copies_payloads_to_the_long_lane_and_removal_deletes_whole_segments` |
| 본문 캐시와 읽기 경로: 고수위에서 놓기만, 콜드 본문은 읽어서 보냄, 줄 머리 미리 읽기, 복구는 메타만(2.5, M3) | `cached_payloads_are_released_at_the_watermark_and_go_out_through_a_read`, `recovery_holds_metadata_only_and_payloads_come_back_through_reads` |
| `Body` 흘려 읽기: 후속은 본문 위치를 가리키고 재기동을 넘음, 디스크로 받을지의 판단(2.6 4번) | `streamed_follow_ups_point_at_their_body_and_come_back_pointing_at_it`, `streamed_follow_up_bodies_go_out_from_disk_and_survive_a_restart`, `body_to_disk_says_yes_at_the_watermark_and_never_without_a_spool`, `a_streamed_body_reads_back_and_its_segment_waits_for_it` |
| M8-4: 이주·삭제·콜드 읽기·체크포인트 훑기가 겹치고 훑는 도중에 죽어도 잃는 것이 없다 | `relocation_removal_reads_and_a_walk_overlap_and_a_crash_mid_walk_loses_nothing` |
| 장부 정합성(속성): 한 이벤트는 한 자리, 줄·참여·회계의 합이 맞다, 엔벨롭의 워커 uuid는 착수 기록과 같다 | `random_operations_keep_the_ledgers_bookkeeping_consistent` |

## 타입 라우팅과 UUIDv7 채널 (계획 9.14, v4.9)

| 요구 | 테스트 |
| --- | --- |
| 채널은 UUIDv7만: 등록·참여·구독·삭제 결과 채널 | `a_channel_is_valid_only_as_a_uuidv7`, `a_channel_that_is_not_a_uuidv7_is_refused_everywhere`, `a_join_without_a_valid_selection_is_malformed` |
| 워커는 채널, 타입, 또는 둘로 참여한다. 참여의 이름은 `req`, 겹침과 이름 충돌은 거절 | `a_typed_worker_takes_one_type_from_fresh_channels_and_answers_on_each`, `a_join_name_in_use_is_refused_for_another_selection_and_leaves_nothing_behind` |
| 권한은 타입으로: 참여·등록·구독이 넘기는 것 | `a_join_needs_the_claim_method_and_nothing_narrower`, `grants_are_methods_and_a_subscription_hands_over_every_type`, `registering_the_servers_own_type_is_malformed` |
| 엔벨롭 38 B와 워커 uuid, 이름 없음, 은퇴한 기능 비트 예약 | `the_worker_uuid_travels_in_assignments_only`, `a_38_byte_head_with_not_before_and_a_worker_decodes_only_as_an_assignment`, `an_assignment_needs_a_worker_and_a_request_never_carries_one`, `no_record_defines_a_name_and_the_retired_tags_are_unknown`, `the_retired_feature_bits_are_reserved` |

## 최소 서버 (계획 9.15, v5)

| 요구 | 테스트 |
| --- | --- |
| 착수 = (이벤트 id, 워커 uuid)(계획 9.16): 배정에 워커 uuid와 lease, nil 워커의 착수 요청은 거절·보고는 오류, 둘이 맞아야 완료·연장·반환, 맞는 실패 보고는 워커 uuid를 지우고 실패 횟수를 올려 다른 워커가 착수, 재기동을 넘음 | `a_join_or_claim_without_a_worker_uuid_is_refused`, `a_report_without_a_worker_uuid_is_malformed`, `a_matching_failure_report_removes_the_worker_and_raises_the_count_for_another_worker`, `a_failure_report_with_the_wrong_worker_is_stale_and_keeps_the_claim`, `a_claim_is_the_event_and_the_worker_uuid_and_a_matching_failure_report_frees_it_for_another_worker`, `a_failure_report_naming_the_event_and_its_worker_frees_it_for_another_worker`, `an_assignment_carries_the_whole_event_and_the_worker_uuid`, `a_report_naming_another_worker_is_stale_and_changes_nothing`, `the_claim_names_the_worker_uuid_and_only_that_uuid_completes_across_a_restart`, `the_worker_uuid_travels_in_assignments_only` |
| 실패는 세기만 하고 서버는 아무것도 하지 않는다. 한도는 워커의 것 | `failures_keep_counting_and_the_server_never_acts_on_them`, `failures_are_counted_and_the_server_never_acts_on_them_across_a_restart`, `the_retry_limit_is_the_workers_the_server_only_counts_failures` |
| 아무도 멈추지 않고 거절되지 않는다. 고수위에서는 캐시된 본문만 내린다 | `memory_past_the_watermark_refuses_and_pauses_nothing`, `nothing_pauses_a_producer_while_nobody_consumes`, `a_full_server_refuses_nothing_and_holds_up_no_producer` |
| 권한은 메서드뿐 | `a_join_needs_the_claim_method_and_nothing_narrower`, `grants_are_methods_and_a_subscription_hands_over_every_type`, `a_producer_that_may_register_nothing_is_refused_before_welcome` |
| 이름 없는 wire v6: 사전·정지 태그 없음, 착수 요청·보고가 워커 uuid를 싣는다 | `claim_requests_and_reports_carry_the_worker_uuid`, `no_record_defines_a_name_and_the_retired_tags_are_unknown`, `a_38_byte_head_with_not_before_and_a_worker_decodes_only_as_an_assignment`, `an_assignment_needs_a_worker_and_a_request_never_carries_one`, `the_retired_feature_bits_are_reserved` |

## 코어 단위 (`crates/ruvent/src/**/tests.rs`, `src/engine/state/tests/`)

| 영역 | 테스트 |
| --- | --- |
| 엔벨롭 크기·바이트(u16 flags, 가변 길이 정수) | `the_head_is_38_bytes_and_the_key_adds_four_plus_a_varint`, `an_assigned_event_has_these_exact_bytes`, `a_dispatch_delivery_has_these_exact_bytes`, `varints_round_trip_and_refuse_overlong_encodings` |
| 레코드 종류별 규칙 | `layouts_refuse_what_they_must_not_carry`, `not_before_is_claimable_only`, `the_decoder_refuses_forbidden_combinations_reserved_bits_and_leftovers`, `every_envelope_round_trips_through_every_layout_that_accepts_it`, `arbitrary_bytes_never_panic_the_decoder` |
| 실패 횟수 포화, id | `failures_saturate_at_65535`, `ids_are_uuids_and_nil_means_unassigned` |
| 이름표 | `equal_texts_share_one_handle`, `one_off_names_do_not_grow_the_dictionary_without_bound`, `a_name_held_elsewhere_survives_a_sweep` |
| 저널 형식 4(레코드 집합, 세그먼트 컨테이너, 체크포인트 파일, 스트리밍 복구) | `every_record_kind_survives_a_reopen_and_payloads_stay_on_disk`, `a_claim_removed_in_the_same_frame_is_not_written_and_keeps_its_serial`, `a_second_engine_cannot_open_a_log_in_use`, `a_single_file_or_older_journal_is_refused_and_left_byte_for_byte`, `a_torn_last_frame_is_cut_and_everything_before_it_recovered`, `damage_in_the_middle_of_the_log_stops_recovery_instead_of_cutting_it`, `a_completion_frame_cut_at_any_byte_recovers_all_or_nothing`, `segments_roll_at_their_size_and_each_decodes_on_its_own`, `recovery_reads_the_checkpoint_and_the_tail_from_its_start`, `an_unfinished_checkpoint_is_ignored_and_the_previous_one_read`, `a_damaged_checkpoint_stops_recovery` |
| 운영 스냅샷 | `a_snapshot_file_holds_both_kinds_and_reads_back`, `the_configuration_picks_types_on_every_channel`, `chosen_types_are_collected_into_snapshot_files` |
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
| 9.5 (거절은 안 보낸다) | `a_full_server_refuses_nothing_and_holds_up_no_producer` |

## 프로토콜 (`crates/ruvent-protocol/src/tests.rs`)

wire v3(2.11): 레코드 = `[tag][head_len][body_len][head][body]`, 바깥 프레임 없음.

| 영역 | 테스트 |
| --- | --- |
| 고정 바이트 | `a_hello_record_has_these_exact_bytes`, `a_register_carries_its_channel_inline_and_its_payload_as_the_body_with_no_names` |
| 왕복(2.11의 모든 레코드, 속성 시험) | `every_call_and_every_server_message_round_trips`, `arbitrary_calls_round_trip`, `an_engine_welcome_is_sent_with_this_version_and_the_encoders_features` |
| `Complete` 뒤 `FollowUp` 잇기 | `a_completion_is_handed_over_only_when_its_last_follow_up_arrived` |
| `Assign`의 남은 lease(쓰기 시작 순간) | `an_assignment_carries_the_lease_left_at_the_moment_it_is_written` |
| 거절 | `a_record_from_the_wrong_direction_or_out_of_place_is_refused`, `unknown_tags_leftover_bytes_bodies_on_bodiless_records_and_forbidden_heads_are_refused`, `arbitrary_bytes_never_panic_the_decoder` |
| 거절된 메시지는 사전 정의도 남기지 않음 | `a_refused_message_writes_nothing` |
| 머리 먼저 읽기·본문 직접 수신·본문 상한(B13, S11) | `records_are_read_head_first_and_each_body_into_its_own_buffer`, `a_body_over_the_bound_is_refused_from_its_header` |
| 벡터 쓰기: 큰 본문은 공유 항목, 작은 본문은 머리 버퍼(S11), 부분 쓰기 잇기 | `a_large_body_is_its_own_shared_vector_entry_and_a_small_one_is_inlined`, `a_partial_write_continues_where_it_stopped` |
| v1 거절(B12) | `version_1_is_refused` |

## 서버 (`crates/ruvent-server/`)

`tests/remote.rs`는 실제 TCP다. 클라이언트가 보내지 않을 것(재전송, 빈자리, 읽지 않는 소켓, v1)은 원시 wire v3 연결로 보낸다.

| 영역 | 테스트 |
| --- | --- |
| 설정(8장 값 전부, 기본값) | `the_example_configuration_parses_into_grants_and_every_section`, `a_minimal_configuration_takes_every_default`, `bad_configurations_are_refused_with_a_reason` |
| 미송신 대기열: 누적 확인 접기, `Fault` 첫 1회, 바이트 상한(S7), 미송신 `Assign` 반환 | `a_newer_committed_replaces_the_queued_one`, `a_fault_is_told_once_per_kind_on_one_connection`, `an_observer_past_its_byte_bound_is_slow_and_bytes_count_until_written`, `dropping_assignments_returns_their_event_ids_and_keeps_the_rest_in_order` |
| 역할별 흐름(2.2) | `each_role_does_its_own_work_over_tcp` |
| 자격·권한(2.7) | `a_wrong_token_and_a_forbidden_method_are_refused_with_their_reason` |
| 재전송 무시, 누적 확인(2.3) | `a_retransmission_is_dropped_quietly`, `acknowledgements_are_cumulative_not_one_per_registration` |
| 순번 빈자리 → `Fault` + `Bye{protocol}` | `a_sequence_gap_is_told_once_and_closes_the_connection` |
| nonce 같음·다름(`superseded`, M8-7) | `the_same_nonce_reconnects_quietly_and_another_nonce_supersedes` |
| 창 배정과 완료(2.4) | `a_window_takes_work_up_to_its_size_and_a_completion_frees_a_slot` |
| 큰 본문 직접 수신 | `a_large_body_arrives_whole` |
| 작업 연결 흘려 읽기(2.6 4·5번, B15) | `a_work_connection_streams_follow_up_bodies_to_disk_under_pressure` |
| 관찰 연결 바이트 상한 → `Bye{slow}`(S7) | `an_observer_that_does_not_read_is_sent_bye_slow` |
| drain 중 송신: 최종 확인을 놓친 연결 0(M8-5, B22) | `draining_while_producers_send_misses_no_final_acknowledgement` |
| 착수 중 정상 재시작 뒤 같은 워커 uuid의 완료(M8-6, 원시 연결, 닫지 않는 피어 포함) | `a_claim_survives_a_graceful_restart_and_completes_under_its_worker_uuid` |
| 쓰지 못한 `Assign`은 즉시 대기로(M8-2) | `assignments_never_written_go_back_at_once_when_the_worker_goes` |
| 단계로 닫기와 닫기 기한(2.2, S20) | `a_bye_is_read_whole_even_when_the_peer_had_sent_more_than_the_server_read`, `a_peer_that_never_closes_after_bye_is_closed_at_the_deadline` |
| v1·다른 버전 거절(B12) | `version_1_and_other_versions_are_refused` |
| keepalive(S13) | `a_tuned_socket_has_keepalive_and_no_delay` |
| 대기분 이어받기 | `a_backlog_larger_than_one_page_is_read_to_the_end_by_subscribing_again` |
| 서버가 연결을 놓을 때 | `when_the_server_goes_away_the_inbox_closes_and_calls_fail_closed` |

## 클라이언트 SDK (`crates/ruvent-client/`)

단위(`src/**/tests.rs`, `src/**` 안의 `mod tests`: 순수 상태, 네트워크 없음) → 통합(`tests/producer.rs`·`tests/worker.rs`:
프로세스 안 엔진(`link::Local`)과 실제 `ruvent-server`(TCP), 연결을 끊는 TCP 중계기) 순이다.

### 단위

| 영역 | 테스트 |
| --- | --- |
| 미확인 버퍼: 누적 확인, `resume` 재개, 기록을 잃은 서버의 다시 번호 매기기(2.3) | `a_cumulative_acknowledgement_empties_the_buffer_up_to_it`, `resume_drops_what_was_applied_and_keeps_the_numbers_of_the_rest`, `a_server_that_lost_the_record_gets_the_rest_numbered_from_resume_plus_one` |
| 재접속 백오프: 지수 + full jitter, `draining`도 같은 상한(S20) | `back_off_grows_to_its_cap_with_full_jitter` |
| 재시도 예산(S9) | `retries_are_bounded_by_a_fraction_of_ordinary_sends` |
| 기본 선택기(B19, B26) | `the_default_selector_prefers_fewer_unacknowledged_bytes` |
| 서버 사이 창: 불변식과 둘 곳 없는 슬롯 | `slots_with_no_place_go_to_the_smallest_connected_window_and_never_exceed_p` |
| 서버 사이 창: 규칙 하나(굶는 서버, 한가한 창, 끝난 서버의 2 차이) | `a_starving_server_takes_one_slot_from_the_largest_idle_window`, `a_run_end_moves_a_slot_only_when_the_ender_is_two_above_the_receiver` |
| 서버 사이 창: 포화와 P < S | `saturation_evens_the_windows_to_within_one_and_holds_the_invariant`, `p_below_s_reaches_a_zero_window_server_as_soon_as_a_slot_goes_idle` |
| 서버 사이 창: 끊긴 서버, 셈 어긋남, 엇갈림, 종료 | `a_disconnected_server_keeps_what_its_runs_hold_and_frees_the_rest`, `a_pending_with_a_known_idle_window_says_the_value_again`, `an_assignment_past_the_window_overflows_and_a_closed_book_opens_nothing` |

### 생산자 (`tests/producer.rs`)

| 영역 | 테스트 |
| --- | --- |
| 누적 확인으로 버퍼가 빈다(S8, B2, TCP) | `cumulative_acknowledgements_empty_the_unacknowledged_buffer_over_tcp` |
| 재접속 뒤 `resume`에서 잇고 중복 0(2.3, TCP 중계기 양쪽 끊기) | `a_reconnection_resumes_after_welcome_resume_with_no_duplicates` |
| 서버 재기동: 저널 있음은 재개, 없음은 다시 번호 매기기 | `a_restart_with_the_journal_resumes_and_one_without_it_renumbers` |
| `Paused`는 그 채널만 쥐고 `Resumed`에 푼다(2.6) | `nothing_pauses_a_producer_while_nobody_consumes` |
| 같은 id 두 인스턴스: `superseded`, 되풀이 없음(M8-7) | `a_superseded_producer_stops_and_the_other_goes_on_without_flapping` |
| 같은 nonce의 비대칭 재접속 3회 멈춤 0(M8-7) | `the_same_nonce_reconnects_three_times_through_a_one_sided_cut_without_stalling` |
| `Bye` 사유표: 토큰 거절은 다시 붙지 않고 오류로 | `a_refused_token_ends_the_producer_with_its_reason` |
| `draining`: 최종 확인 뒤의 나머지를 다른 서버로, 한 번씩(2.9) | `a_draining_server_hands_its_unapplied_rest_to_another_server_once` |

### 워커 (`tests/worker.rs`)

| 영역 | 테스트 |
| --- | --- |
| 서버 2·4대 90/10 편중: P 초과 실행 0, 바쁜 서버 창 ≥ 1, 중복 실행 0(M1) | `skewed_load_over_two_and_four_servers_never_runs_past_p_and_keeps_every_busy_window_open` |
| P < S, 간헐 유입(M1) | `workers_with_fewer_slots_than_servers_reach_every_server` |
| 강제 종료된 워커의 일이 재접속 유예 안에 재배정(M8-2, TCP) | `a_killed_worker_is_reassigned_within_the_reconnect_grace` |
| 서버 정상 재시작 중 착수 유지: 재배정 0, 실패 횟수 변화 0(M8-6) | `a_graceful_restart_keeps_the_run_claimed_and_counts_nothing` |
| 하류 장애 "나중에 다시"는 실패로 세지 않고, "이벤트 탓"은 세며 한도는 핸들러가 정한다(M8-12, B23, 9.15) | `the_retry_limit_is_the_workers_the_server_only_counts_failures` |
| 종료 절차: 창 0 → 유예 → 반환(실패 아님) | `shutdown_closes_the_windows_and_gives_back_what_outlives_the_grace` |
| 실행 중 방출은 완료보다 먼저(2.2) | `a_dispatch_emitted_while_running_comes_before_the_completions_follow_up` |

## 프록시 (`crates/ruvent-proxy/`)

프록시 크레이트는 골조다(4장): 서버별 상향과 구독 규칙만 있고, 클라이언트 쪽 프로토콜·인증·설정 파일·요청–응답·기한·역압은
프로젝트의 것이다. 단위(`src/**/tests.rs`: 순수 상태, 네트워크 없음) → 통합(`tests/fanout.rs`: SDK `Local`로 붙은 같은
프로세스의 엔진) → 인수(`tests/acceptance.rs`: 실제 `ruvent-server`에 SDK `Tcp`로 붙은 상향) 순으로 쌓는다. 인수 테스트는
판정하는 원문(개편 계획 4장)을 `/// >`로 인용한다.

### 단위

| 범주 | 테스트 |
| --- | --- |
| 구독 장부: 가입, ticket, 효력과 답 | `a_join_waits_for_its_own_confirmation_and_its_answer`, `the_answer_and_the_confirmation_settle_in_either_order`, `every_join_issues_its_own_subscribe`, `an_earlier_confirmation_does_not_activate_a_later_joiner`, `joining_twice_is_answered_at_once_without_a_call`, `a_second_join_while_waiting_waits_with_the_first` |
| 구독 장부: 서버별 효력과 재접속 | `a_user_is_in_effect_per_server`, `a_reconnection_resets_a_server_until_it_confirms_again`, `a_join_with_no_server_connected_waits_for_the_first_confirmation` |
| 구독 장부: 요청하지 않은 확인·답 | `confirmations_and_answers_nobody_asked_for_are_ignored` |
| 구독 장부: 탈퇴와 `0→1→0→1` | `only_the_last_user_leaving_unsubscribes`, `leaving_before_the_confirmation_answers_the_waiting_join`, `resubscribing_waits_for_the_second_subscribes_confirmation`, `leaving_all_leaves_every_channel_and_unknown_ones_do_nothing` |
| 구독 장부: 거절 | `a_refusal_fails_the_join_it_answers` |
| 구독 장부: 채널마다 따로 | `a_member_hears_only_its_own_channel` |
| 준비 상태 계산 | `ready_needs_every_listed_server_observed`, `an_empty_list_is_not_ready_and_unlisted_servers_do_not_count` |

### 팬아웃 (`tests/fanout.rs`)

| 범주 | 테스트 |
| --- | --- |
| 구독 집약과 효력 시점 | `three_members_on_one_channel_cost_one_upstream_delivery`, `a_late_joiner_gets_only_events_after_it_joined`, `resubscribing_never_shows_events_from_the_gap`, `the_last_member_leaving_cancels_the_upstream_subscription`, `after_the_upstream_drops_the_fanout_resubscribes`, `a_subscription_made_before_the_upstream_is_up_takes_effect_when_it_is`, `leaving_before_the_subscription_took_effect_answers_left` |
| 서버 목록을 따르기 | `a_server_added_to_the_list_is_subscribed_for_every_member` |
| 느린 구성원 분리, 원 엔벨롭 공유 | `a_slow_member_is_detached_and_nobody_else_is`, `members_share_the_envelopes_as_the_server_sent_them` |
| 허브 수명과 상향 해제 | `stopping_the_hub_ends_every_stream_and_lets_go_of_the_upstream`, `dropping_every_handle_stops_the_hub` |

### 인수 (`tests/acceptance.rs`, 실제 `ruvent-server`)

| 판정하는 원문 | 테스트 |
| --- | --- |
| 4장 "신규 join은 자기 구독 표지를 기다리고, 이미 활성인 같은 사용자의 재호출은 즉시 성공한다" | `the_subscription_rule_holds_through_a_real_event_server` |
| 9.14 "난 채널이 특수한 이름이나 식별자를 갖길 원하지 않고 uuid7에 근접한 일관된 형식만 받아주길 원해" | `a_channel_that_is_not_a_uuid_v7_is_refused_through_a_real_event_server` |
| 4장 "상향 연결은 서버마다 역할별로 하나다 … 모든 서버에 붙도록" (서버 2대, 양쪽 발송) | `a_fanout_on_two_servers_hands_members_what_either_dispatches` |
| 4장 "조용히 끊긴 상향 연결이 `/health`에 정상으로 남지 않게 한다" (서버 하나 상실) | `an_upstream_that_goes_away_shows_in_health` |
| 4장 "요청–응답은 구성원 하나다. 요청마다 응답 채널을 구독하고 … 오는 것을 클라이언트에 쓴다" (요청 2건 동시, 실제 워커) | `each_request_is_answered_on_its_own_channel_in_order` |

FIN 없는 조용한 끊김(M8-8)은 프로세스 안에서 만들 수 없다. 사용자 공간의 중계는 커널이 keepalive에 답하므로 끊김으로
보이지 않는다. 그 감지는 SDK가 모든 상향 소켓에 거는 커널 keepalive(S13)의 몫이고, 인수 테스트는 서버가 연결을 끝내는
경우로 준비 상태 반영을 판정한다.
