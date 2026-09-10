# PR #36 counterexample matrix — 2026-09-10

Verified source revision: `1d297a2ddbf03ae128d4fe0135462ae4c000170a`

Database verification: PostgreSQL 17.6, fresh migrations `001..009`, then a second apply of `009`

“RED observed” records the actual corrective TDD run. T04 and T09 were positive/control semantics that already passed on the reviewed PR head; they are reported honestly rather than fabricating a RED. Every row below has an exact semantic test and passed in the final PostgreSQL or pure-contract run.

| ID | RED observed | Minimal fix | GREEN observed | Exact test |
|---|---|---|---|---|
| T01 | Invalid event type was not constrained to the exact universe | Exact event-type CHECK | INVALID rejected | `test_T01_invalid_event_type_is_rejected` |
| T02 | Event/observation ontology mismatch was storable | Registry FK for official observations | CPI + FED observation rejected | `test_T02_cpi_with_fed_observation_is_rejected` |
| T03 | Event/consensus ontology mismatch was storable | Registry FK for consensus | CPI + FED consensus rejected | `test_T03_cpi_with_fed_consensus_is_rejected` |
| T04 | Baseline control already GREEN | No semantic change beyond exact ontology registry | FOMC + FED observation accepted | `test_T04_fomc_with_fed_observation_is_accepted` |
| T05 | Lifecycle allowed an invalid backward transition | Append-only transition trigger | RELEASED → SCHEDULED rejected | `test_T05_released_to_scheduled_transition_is_rejected` |
| T06 | Required lifecycle chain lacked exact enforcement/current selection | Transition trigger + current view | SCHEDULED → RESCHEDULED → RELEASED accepted; v3 current | `test_T06_scheduled_rescheduled_released_chain_is_accepted` |
| T07 | Observation identity did not conflict on changed publication time | Record function compares material fields | Different `published_at` conflicts | `test_T07_same_observation_revision_different_published_at_conflicts` |
| T08 | Revision type was not part of material conflict | Record function compares `revision_type` | REVISION vs CORRECTION conflicts | `test_T08_same_observation_revision_different_revision_type_conflicts` |
| T09 | Baseline control already GREEN | Preserved source-revision uniqueness | Same source revision across revisions conflicts | `test_T09_source_revision_id_cannot_name_different_revisions` |
| T10 | Consensus lacked exact idempotent write boundary | Provider-local record function | Same identity/content returns same row | `test_T10_same_consensus_identity_and_content_is_idempotent` |
| T11 | Consensus material conflict was not exact | Record function compares material fields | Different value/hash conflicts | `test_T11_same_consensus_identity_different_content_conflicts` |
| T12 | Selector could mix providers | Provider required by selector signature/query | Vendor A selected despite later Vendor B | `test_T12_selector_is_scoped_to_explicit_provider` |
| T13 | Marker-time snapshot could enter pre-release selection | Strict `snapshot_at < marker_at` | Equal timestamp excluded | `test_T13_consensus_at_marker_time_is_excluded` |
| T14 | Impossible observation timing was accepted | `first_observed_at >= snapshot_at` CHECK | Earlier observation rejected | `test_T14_consensus_first_observed_before_snapshot_is_rejected` |
| T15 | Corrected marker had no durable revision/current rule | Append-only marker revision chain + current view | v1/v2 retained; v2 current | `test_T15_corrected_marker_is_new_revision_and_current` |
| T16 | More than one current primary marker was possible | Primary/current trigger constraint | Second current primary rejected | `test_T16_two_current_primary_markers_are_rejected` |
| T17 | Surprise could use an initial actual inconsistent with current marker | Surprise input trigger pins initial actual/current marker | Pre-marker published actual rejected | `test_T17_initial_actual_published_before_current_marker_is_rejected_for_surprise` |
| T18 | Uncontracted standardized surprise was accepted | NULL-only CHECK | Non-null standardized value rejected | `test_T18_standardized_surprise_must_be_null` |
| T19 | Planner accepted unsupported market codes | Exact `US_EQUITIES` validation | XNYS rejected | `test_T19_rejects_unsupported_market_code` |
| T20 | Planner accepted caller-controlled timezone | Exact `America/New_York` validation | UTC rejected | `test_T20_rejects_wrong_market_timezone` |
| T21 | Calendar snapshot parent was mutable | Append-only trigger | Metadata UPDATE rejected | `test_T21_calendar_snapshot_metadata_is_append_only` |
| T22 | Post-market S0 semantics needed an exact named counterexample | Exact phase-to-session mapping retained | S0 is next trading session | `test_T22_post_market_maps_next_session_to_reaction_s0` |
| T23 | `EVENT_TO_CLOSE` applicability was too coarse | Phase-specific resolver | Post-market returns NOT_APPLICABLE/no endpoints | `test_T23_post_market_event_to_close_is_not_applicable` |
| T24 | POST_5M endpoint semantics were not exact | Marker-minus-one through marker-plus-four close | 08:29 close → 08:34 close | `test_T24_0830_post_5m_uses_0829_close_to_0834_close` |
| T25 | Release-to-open end reference was ambiguous | Explicit last-valid-pre-open endpoint | 08:29 close → last valid pre-open close | `test_T25_release_to_open_ends_at_last_valid_pre_open_close` |
| T26 | Open-gap boundary/price types were ambiguous | Explicit pre-open close → S0 open | Correct close/open endpoints | `test_T26_open_gap_uses_pre_open_close_to_0930_open` |
| T27 | OPEN_30M ended one minute late | S0 open through +29-minute close | 09:30 open → 09:59 close | `test_T27_open_30m_uses_0930_open_to_0959_close` |
| T28 | Reaction identity lacked marker identity | Marker ID added to identity | Statement and press POST_5M remain distinct | `test_T28_marker_id_is_part_of_reaction_identity` |
| T29 | Normal success without reason was rejected by coarse reason rules | Reason pairing permits both fields absent | Normal success accepted | `test_T29_normal_quality_success_without_reason_is_valid` |
| T30 | DATA_NOT_AVAILABLE could claim COMPLETE | Cross-taxonomy validation | Invalid combination rejected | `test_T30_data_not_available_with_complete_coverage_is_rejected` |
| T31 | SKIPPED could claim ELIGIBLE | Cross-taxonomy validation | Invalid combination rejected | `test_T31_skipped_work_item_cannot_be_eligible` |
| T32 | Validation run ID could be reused with changed checkpoint | Immutable run parent + conflict function | Changed checkpoint rejected | `test_T32_same_validation_run_different_checkpoint_is_rejected` |
| T33 | Validation replay depended on mutable UPSERT | Append-only idempotent record function | Same bar content returns same row | `test_T33_same_reconstructed_bar_content_is_idempotent` |
| T34 | Same reconstructed-bar identity could overwrite OHLC | Determinism conflict function; UPDATE removed | Different OHLC rejected | `test_T34_same_reconstructed_bar_identity_different_ohlc_conflicts` |
| T35 | No current surprise projection existed after a marker correction | Marker lineage on immutable surprise history plus dynamic current view | Old surprise remains historical but is absent from current projection | `test_T35_marker_correction_removes_stale_surprise_from_current_projection` |
| T36 | A later eligible provider snapshot could leave an older surprise looking current | Current view re-runs the provider-local strict pre-marker selector | Old surprise remains historical but is absent from current projection | `test_T36_later_eligible_consensus_makes_old_surprise_noncurrent` |
| T37 | Stable canonical event identity fields were directly mutable | Identity-only UPDATE trigger; locator URL remains mutable | Event-type mutation rejected | `test_T37_canonical_event_identity_fields_are_immutable` |
| T38 | DB accepted a session whose timestamps belonged to another New York date | Parent-snapshot timezone trigger | Local-date mismatch rejected | `test_T38_session_timestamp_local_date_must_match_session_date` |
| T39 | Late regular-session POST_30M crossed the close and remained eligible | Typed definition-derived endpoint plus close guard | Returns `NOT_APPLICABLE` with no endpoint | `test_T39_late_regular_post_metric_cannot_cross_session_close` |
| T40 | Raw phase typo could bypass phase policy | One canonical `ReleasePhase` enum at resolver boundary | `POSTMARKET` rejected | `test_T40_arbitrary_release_phase_is_rejected` |
| T41 | `NO_OBSERVATIONS + ELIGIBLE` was accepted | Explicit cross-taxonomy guard | Invalid state rejected | `test_T41_no_observations_cannot_be_eligible` |
| T42 | Abnormal quality states could omit reason metadata | Exact normal tuple exemption plus abnormal reason rule | Failed/partial/insufficient without reason rejected | `test_T42_abnormal_quality_state_requires_reason` |
| T43 | Deterministic two-transaction race committed two different primary kinds | Event-scoped transaction advisory lock before marker-chain checks | Exactly one commit; loser receives domain conflict | `test_T43_concurrent_primary_marker_insert_has_one_domain_winner` |
| T44 | Concurrent identical observations leaked a raw identity UniqueViolation | Event/code transaction advisory lock before read-then-insert | One fact; both callers receive the same ID | `test_T44_concurrent_identical_observations_return_same_id` |
| T45 | Concurrent identical consensus snapshots leaked a raw identity UniqueViolation | Event/code/provider transaction advisory lock | One fact; both callers receive the same ID | `test_T45_concurrent_identical_consensus_returns_same_id` |
| T46 | Concurrent identical validation bars leaked a raw identity UniqueViolation | Full bar-identity transaction advisory lock | One fact; both callers receive the same ID | `test_T46_concurrent_identical_validation_bars_return_same_id` |
| T47 | Concurrent different validation bars leaked a timing-dependent raw UniqueViolation | Same serialized bar identity plus existing material comparison | One fact; loser receives `VALIDATION_BAR_DETERMINISM_CONFLICT` | `test_T47_concurrent_different_validation_bars_use_domain_conflict` |
| T48 | One provider source revision naming two internal revisions leaked an index name | Explicit source-revision lookup under the observation lock | Deterministic `CONFLICTING_SOURCE_FACT` | `test_T48_source_revision_conflict_uses_domain_error` |
| T49 | No pure verified-calendar S+1 seam existed | Snapshot-scoped row-count resolver | Friday +1 resolves to Monday session | `test_T49_session_offset_counts_verified_sessions_after_friday` |
| T50 | No pure verified-calendar S+7 seam existed | Same resolver counts seven stored sessions | Resolves to seventh session, not date +7 | `test_T50_session_offset_seven_uses_verified_calendar_not_date_math` |

Additional exact guards also pass for lifecycle idempotency/conflict, observation material fields, current-marker demotion, corrected-marker consensus selection, marker/session immutability, revised-actual rejection, surprise arithmetic, concurrent validation-run idempotency, typed price/activity definitions, provider/raw physical isolation, and replay recovery.
