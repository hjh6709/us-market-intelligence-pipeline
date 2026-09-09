# Current system architecture

## Product identity

이 저장소는 **U.S. Economic Event Market Intelligence Platform**입니다. 핵심은 데이터 엔지니어링이며, 연구와 Paper execution은 저장 결과를 소비하는 downstream plane입니다.

## Three planes

| Plane | Input | Processing | Durable output |
| --- | --- | --- | --- |
| Research | official releases, Alpaca SIP bars, FRED/ALFRED vintages | Airflow collection, point-in-time selection, derived bars, versioned analysis | events, bars, macro contexts, impacts, strategy observations |
| Validation | archived SIP trades | Kafka replay, Spark event-time validation/dedup/aggregation | verified 1m bars and load/recovery evidence |
| Product | PostgreSQL results, durable telemetry, manual order intent | FastAPI services and browser UI | read views and isolated Alpaca Paper journal |

Research provider bars and archived raw trades are intentionally separate. The 202×10 research dataset was not produced by replaying all raw trades through Kafka/Spark.

## Lineage

```text
official events + market_bars + macro_event_contexts
  -> macro_event_impacts (alpaca/sip/multi_event_sip_v1)
  -> event_strategy_results (pre60_momentum_post60/v1)
  -> ServingRepository -> ServingService -> FastAPI -> browser

pipeline_runs + pipeline_work_items + pipeline_run_checks
  -> PipelineServingRepository -> Pipeline UI

manual browser form -> strict Paper validation -> journal-before-POST
  -> fixed Alpaca Paper endpoint -> GET reconciliation -> browser journal
```

## Failure boundaries

- Provider collection failures, pagination truncation, malformed bars and persistence failures are failures.
- Successful sparse bar responses are successful collections with separate quality metadata.
- Market closure or unavailable future data is an explicit warning/data-not-available state.
- PostgreSQL writes use deterministic business keys and upserts.
- Paper POST is never automatically retried; uncertain outcomes are reconciled with GET.
- Research output never becomes a Paper order input.

See the [interactive Archify diagram](../diagrams/session7-architecture.html) and the application `/pipelines` lineage panel.
