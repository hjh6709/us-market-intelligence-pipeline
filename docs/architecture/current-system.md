# Current system architecture

Status: implementation truth. Historical baseline: `63633a50c85c88e507be458067ecf6706220f813`; the corrective artifact revision is recorded in the dated verification receipt. This page does not describe the approved target; see [platform-contract.md](platform-contract.md).

## Product identity

이 저장소의 정본 제품명은 **Economic Event Intelligence & Strategy Validation Platform**입니다. 핵심은 데이터 엔지니어링이며, 연구와 Paper execution은 저장 결과를 소비하는 downstream plane입니다.

## Three planes

| Plane | Input | Processing | Durable output |
| --- | --- | --- | --- |
| Research | official releases, Alpaca SIP bars, FRED/ALFRED vintages | Airflow collection, point-in-time selection, derived bars, versioned analysis | events, bars, macro contexts, impacts, strategy observations |
| Validation | archived SIP trades | Kafka replay, Spark event-time validation/dedup/aggregation | raw-derived validation bars and load/recovery evidence |
| Product | PostgreSQL results, durable telemetry, manual order intent | FastAPI services and browser UI | read views and isolated Alpaca Paper journal |

Research provider bars and archived raw trades are intentionally separate. The 202×10 research dataset was not produced by replaying all raw trades through Kafka/Spark.

## Corrective foundation now present

Migration 009 adds empty normalized lifecycle/observation/consensus/surprise/calendar/session foundations with exact ontology, append-only revisions, provider-local PIT selection, a dynamic current-surprise projection, transaction-safe immutable writes, and immutable validation lineage. `src/trading_sessions.py` adds a snapshot-owned pure current-marker verified-session planner and S+N resolver; `src/platform_contracts.py` adds exact typed quality/maturity/reaction definitions. These foundations are executable and tested but are not populated by production adapters and do not replace current serving.

Provider-aggregated research bars remain in `market_bars`. Raw-SIP Spark sinks record an immutable `validation_runs` parent and append-only `validation_reconstructed_bars`; conflicting output under the same run/bar identity fails instead of updating evidence. Current research serving queries `market_bars` only. Historical evidence predating this correction remains evidence of its recorded run, not proof that the new table was used then.

## Lineage

```text
official events + market_bars
  -> macro_event_impacts (alpaca/sip/multi_event_sip_v1)
  -> event_strategy_results (pre60_momentum_post60/v1)
  -> ServingRepository -> ServingService -> FastAPI -> browser

official events -> PIT macro_event_contexts -> ServingRepository -> browser

pipeline_runs + pipeline_work_items + pipeline_run_checks
  -> PipelineServingRepository -> Pipeline UI

manual browser form -> strict Paper validation -> journal-before-POST
  -> fixed Alpaca Paper endpoint -> GET reconciliation -> browser journal
```

거시 맥락은 발표별로 연결해 연구 화면에서 함께 읽습니다. 현재 영향 수익률·거래량 계산의 수치 입력은 시장 봉과 발표 구간이며, 거시 맥락을 예측 변수로 사용하는 모델은 구현하지 않았습니다.

시장 수집 Airflow DAG가 `pipeline_*` 기록을 남깁니다. 거시 수집·raw 재생·연도별 trigger의 모든 실행이 이 테이블에 있는 것은 아닙니다. [DAG별 추적 범위와 실패 경계](../engineering/airflow-audit.md)를 함께 확인합니다.

Paper 로컬 기록은 서버에 고정한 `ALPACA_PAPER_ACCOUNT_ID`로 읽고, 브로커 작업 전에 credentials의 계정과 일치하는지 확인합니다. 계정 GET 장애 때문에 로컬 journal까지 숨기지 않습니다.

## Failure boundaries

- Provider collection failures, pagination truncation, malformed bars and persistence failures are failures.
- Successful sparse bar responses are successful collections with separate quality metadata.
- Market closure or unavailable future data is an explicit warning/data-not-available state.
- Legacy curated writes use deterministic business keys and upserts; normalized source facts and validation evidence are append-only with explicit idempotency/conflict functions.
- Paper POST is never automatically retried; uncertain outcomes are reconciled with GET.
- Research output never becomes a Paper order input.

See the final-presentation [current architecture source](../presentation/final/CURRENT_ARCHITECTURE.mmd) and the application `/pipelines` lineage panel. The older Session 7 Archify artifact is historical and predates the validation-storage isolation now described here.

The approved target diagram is separately published as [target-platform.html](../diagrams/target-platform.html). A target node is not evidence of current implementation; use the [current-versus-target matrix](../engineering/current-vs-target.md).
