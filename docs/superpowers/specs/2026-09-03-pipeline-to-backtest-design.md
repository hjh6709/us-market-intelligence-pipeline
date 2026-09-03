# 경제지표 파이프라인 완성 및 백테스트 설계

기준일: 2026-09-03

상태: 사용자 검토 대기

범위: 자동매매 이전의 데이터 파이프라인·분석·백테스트 완성

## 1. 목표

이 단계의 목표는 경제지표와 시장 데이터를 단순히 저장하는 데서 끝나지 않고, 같은 입력을 다시 실행해도 같은 분석·백테스트 결과가 나오는 하나의 자동화된 데이터 제품을 만드는 것이다.

```text
공식 발표 일정 + 당시 알려진 경제지표 + 시장 데이터
→ Airflow 실행
→ 수집·검증·처리·저장
→ 경제 이벤트와 시장 반응 결합
→ point-in-time event study 및 전략 백테스트
→ 결과·coverage·실패 원인 확인
```

이 단계에서는 실계좌 주문을 실행하지 않는다. 이 설계의 완료 조건을 충족한 뒤 Alpaca Paper Trading, 위험 한도와 kill switch를 별도 단계로 설계한다.

### 현재 구현 경계

이 문서의 설계와 이미 실행된 결과를 혼동하지 않는다.

- 현재 Airflow 구현: `market_sip_replay_pipeline`이 입력받은 여러 ticker의 동일 시간 범위를 Alpaca→Kafka→Spark→PostgreSQL로 처리
- 현재 대규모 원시 replay 증거: CPI 55회 × 4종목, Kafka 파티션 쏠림과 Spark 부하·DB 복구 실험 완료
- 현재 분석용 bar 증거: 77회 × 10종목을 배치 수집해 1m·3m·5m·1d를 PostgreSQL에 저장
- 아직 미구현: 77 × 10 수집기의 Airflow 연결, 역할별 DAG·Asset 연결, `economic_event_metrics`, v2 Kafka key 실험, 전체 event study, 백테스트, 실제 alert 저장

따라서 아래 내용은 이미 된 일을 부풀리는 설명이 아니라, 현재 결과를 다음 구현 단계로 연결하기 위한 확정 설계다.

## 2. 고정 범위

### 경제 이벤트

| 유형 | 현재 확정 범위 | 백테스트 전 보완 범위 | 역할 |
| --- | ---: | --- | --- |
| CPI | 55회(2022~2026) | 현재 범위 유지 | 물가 발표 반응 |
| EMPLOYMENT | 8회(2026) | 2022년 이후 공식 발표로 확장 | 고용·실업률 발표 반응 |
| PCE | 9회(2026) | 2022년 이후 공식 발표로 확장 | PCE 물가 발표 반응 |
| FOMC | 5회(2026) | 2022년 이후 공식 결정으로 확장 | 정책금리 발표 반응 |

현재 수집 완료 범위는 총 77회다. 이는 6차시 과제의 파이프라인 검증에는 충분하지만, 이벤트 유형별 전략 성과를 비교하기에는 CPI 이외 표본이 너무 적다. 따라서 **현재 77회는 과제 제출 기준**, 네 이벤트 유형을 모두 2022년 이후로 맞춘 범위는 **백테스트 시작 기준**으로 구분한다. 공식 발표 시각과 원문 URL이 확인된 이벤트만 catalog에 넣는다.

### 종목

`SPY, QQQ, IWM, TLT, XLF, SMH, GLD, NVDA, AAPL, JPM`의 10종목을 1차 공통 universe로 고정한다. 10종목은 파이프라인 처리와 경제 이벤트별 반응의 차이를 검증하는 데에는 충분하지만 미국 시장 전체에 대한 투자 성과를 일반화하는 표본은 아니다.

- SPY·QQQ·IWM: 시장·성장주·소형주 기준
- TLT·GLD: 금리·위험회피 반응
- XLF·SMH: 금융·반도체 업종
- NVDA·AAPL·JPM: 업종 ETF와 개별 대형주 비교

종목 목록은 `config/market_universe.json`에서 변경할 수 있어야 한다. 실행 결과에는 `universe_version`과 당시 종목 목록을 함께 저장한다. 1차 백테스트가 끝나기 전에는 단순히 표본 수를 늘리기 위해 종목을 추가하지 않는다. 종목별 결과가 지나치게 불안정하거나 업종 대표성이 부족하다는 증거가 있을 때만 universe를 확장하며, 현재 시점에 고른 종목을 과거에도 그대로 사용함으로써 생기는 선택 편향을 결과의 한계로 명시한다.

### 시간 해상도

| 계층 | 범위 | 목적 |
| --- | --- | --- |
| 원시 체결 | 발표 T-60분~T+60분 | Kafka·Spark 전달 및 집계 정합성 검증 |
| 1분봉 | 발표 T-60분~T+120분 | 발표 직전·직후 반응의 기준 데이터 |
| 3분봉·5분봉 | 같은 1분봉에서 파생 | 희소한 장전 구간을 coverage와 함께 비교 |
| 일봉 | 이전 7거래일+발표일+이후 7거래일 | 중기 drift·반전·지속성 분석 |

## 3. 선택한 아키텍처

데이터 구조가 다른 두 경로를 하나의 토픽에 억지로 섞지 않고, Airflow가 두 경로의 실행과 검증을 관리한다.

사용하는 외부 데이터는 이 단계에서 Alpaca SIP, BLS·BEA·Federal Reserve 공식 자료와 FRED/ALFRED로 제한한다. `fdnpy`, Yahoo Finance, 뉴스·심리·옵션 데이터는 품질과 point-in-time 계약을 별도로 검증하기 전에는 혼합하지 않는다.

### 검토한 세 가지 방식

| 방식 | 장점 | 문제 | 결정 |
| --- | --- | --- | --- |
| 하나의 거대한 DAG | 전체 흐름을 한 화면에서 보기 쉬움 | 770개 수집·Spark·분석 실패가 한 run에 얽히고 재실행 범위가 큼 | 사용하지 않음 |
| 단계별 DAG를 수동 실행 | 구현이 단순하고 서로 격리됨 | 선행 데이터 완료 여부와 실행 이력이 사람의 기억에 의존 | 사용하지 않음 |
| 역할별 DAG + 검증된 Asset 연결 | 수집·처리·분석 실패를 분리하고 해당 범위만 재실행 가능 | DAG 간 계약과 run metadata 설계가 필요 | **선택** |

### 원시 체결 경로

```text
Alpaca historical trades 또는 저장된 Parquet
→ Kafka raw.market-sip.v1
→ Spark schema·중복·거래 조건 검사
→ event-time 1분봉 집계
→ PostgreSQL market_bars
→ provider 1분봉과 정합성 비교
```

Kafka·Spark는 개별 체결의 전달, 순서, 중복, event-time 집계와 장애 복구를 검증한다. 77회 × 10종목의 모든 원시 체결을 한 번에 메모리에 올리지 않고 Airflow가 이벤트·종목 파티션 단위로 실행한다.

### Kafka 파티션 쏠림 개선

기존 부하 실행에서는 Kafka key를 `symbol`만 사용해 세 파티션 중 한 곳에 누적 메시지의 약 97.5%가 배정됐다. 이는 Kafka 장애가 아니라 key cardinality가 낮고 거래량이 큰 종목들이 같은 파티션에 hash된 설계 문제다. 파티션 수만 늘려도 같은 key의 모든 메시지는 계속 한 파티션으로 가므로, key 계약을 먼저 고친다.

실시간과 과거 재생의 순서 보장 범위가 다르므로 토픽과 key를 다음처럼 구분한다.

| 경로 | 토픽 | 메시지 key | 보장할 순서 |
| --- | --- | --- | --- |
| 실시간 IEX | `raw.market.v1` | `symbol` | 종목별 연속 거래 순서 |
| 과거 SIP 재생 | `raw.market-sip.load.v2` | `event_id|symbol|segment` | 한 발표·한 종목·시간 조각 내부 순서 |

`segment`는 발표 기준 15분 단위의 결정적 구간 번호다. `event_id|symbol`만 사용하면 전체 backfill에서는 최대 770개 key가 생기지만, 한 이벤트·한 종목만 재실행할 때는 여전히 모든 메시지가 한 파티션으로 간다. 과거 replay는 전역 도착 순서가 아니라 event time으로 집계하므로 121분을 약 9개 segment로 나눠 한 구간 안의 순서만 유지한다. `event_id`, `symbol`, 원래 거래 ID와 거래 시각은 JSON payload에도 그대로 보존한다. key 계약이 달라졌음을 명확히 하기 위해 기존 부하 증거의 `v1` 토픽을 덮어쓰지 않고 `v2` 토픽을 사용한다.

Producer는 `enable.idempotence=true`, `acks=all`과 호환되는 설정을 명시적으로 사용하고 delivery callback의 성공 offset만 manifest에 넣는다. Kafka 보관 기간 24시간은 영구 원본 보관이 아니라 빠른 재처리 창이다. 재현 가능한 원본은 request scope·page count·row count·SHA-256이 기록된 Git 제외 Parquet archive로 보존한다. 복구가 24시간을 넘을 수 있는 운영 환경에서는 측정된 최대 복구시간보다 Kafka retention을 길게 잡거나 archive에서 새 토픽으로 replay한다.

현재 Spark SIP 배치는 Kafka의 시작·종료 offset을 manifest로 고정해 읽으므로 consumer group의 자동 offset에 의존하지 않는다. 건수 확인용 Consumer는 실행마다 고유 group을 사용한다. 향후 `v2`를 Structured Streaming으로 읽을 때에만 `market-sip-v2-processor`처럼 별도 consumer group과 별도 checkpoint 경로를 만든다. 기존 `v1` checkpoint를 재사용하지 않는다. Spark checkpoint에는 처리한 offset과 집계 상태가 들어가므로 토픽·key·query 구조가 바뀐 실행과 섞으면 복구 의미가 불명확해지기 때문이다.

파티션 수는 추측으로 늘리지 않고 같은 입력으로 3개와 6개를 비교해 결정한다.

- 파티션별 메시지 건수와 byte 비율
- 최대 파티션 비중과 최소 파티션 비중
- Producer 처리량과 전송 시간
- Spark input rows/sec와 batch duration
- Consumer lag와 전체 완료 시간
- Spark peak memory와 disk spill

분포 판정은 `최대 파티션 건수 / 파티션 평균 건수`를 사용한다. 이 값이 1.5 이하이고 Spark가 목표 처리량을 따라가면 3개를 유지한다. 1.5를 넘거나 지속적인 lag가 발생하고, 6개에서 전체 완료 시간이 20% 이상 개선되며 메모리 오류가 없다면 6개로 변경한다. 이 1.5와 20%는 Kafka의 공식 기준이 아니라 이 프로젝트의 사전 acceptance criterion이다. 로컬·GCP 실행 코어보다 무조건 많은 파티션을 만드는 것은 병렬성을 보장하지 않고 관리 비용만 늘릴 수 있으므로 피한다. 단일 Kafka broker 환경에서 파티션 증가는 처리 병렬성 조정이며 복제·고가용성 개선이 아니라는 점도 문서에 표시한다.

변경 후에는 다음을 반드시 검증한다.

1. 같은 `event_id|symbol|segment`의 offset 순서가 유지된다.
2. Producer 발행, Consumer 수신과 Spark 입력 총건수가 일치한다.
3. 파티션별 분포가 실행 evidence에 기록된다.
4. 동일 입력 재실행에서 최종 DB 행 수와 결과 hash가 변하지 않는다.
5. 기존 `symbol` key로 만든 97.5% 쏠림 결과와 변경 후 결과를 같은 표에서 비교한다.

### 분석용 bar 경로

```text
Alpaca historical SIP 1Min·1Day
→ Airflow event × symbol 동적 작업
→ schema·범위·coverage 검사
→ 1분봉에서 3분봉·5분봉 파생
→ PostgreSQL market_bars Upsert
```

Alpaca가 이미 집계한 bar를 raw trade 토픽에 넣지 않는다. 이 경로는 배치 backfill과 분석용 coverage를 담당한다.

### 경제지표 경로

```text
BLS·BEA·Federal Reserve 공식 발표 catalog
+ FRED·ALFRED 10개 series의 당시 이용 가능 값
→ Airflow
→ economic_events + macro_event_contexts
→ 시점 정합성 검사
```

현재 정의된 series는 `CPIAUCSL, CPILFESL, PCEPI, PCEPILFE, UNRATE, PAYEMS, DFF, DGS2, DGS10, VIXCLS`다. 이 10개 값은 모두 해당 이벤트의 **발표 결과**가 아니라 발표 직전 시장이 알고 있던 **배경 정보**다. 월별 series는 발표일 당시 유효한 vintage를 선택하고, 일별 금리·VIX는 발표 당일 종가가 섞이지 않도록 발표 전날까지의 값만 사용한다.

별도의 `economic_event_metrics`에는 그날 실제 공개된 결과를 저장한다. 한 발표에는 여러 값이 있으므로 `economic_events.actual` 한 칸에 억지로 넣지 않는다.

| event type | 공개 결과 metric 예시 | 1차 출처 |
| --- | --- | --- |
| CPI | headline MoM·YoY, core MoM·YoY | BLS archived release + ALFRED vintage |
| EMPLOYMENT | nonfarm payroll change, unemployment rate | BLS archived release + ALFRED vintage |
| PCE | headline/core PCE MoM·YoY | BEA archived release + ALFRED vintage |
| FOMC | target lower bound, target upper bound, 결정 시각 | Federal Reserve statement/implementation note |

검증된 전망치 제공자가 없으므로 `forecast`와 `surprise`는 만들지 않는다. ALFRED 값은 `vintage_dates=release_date` 또는 동일한 real-time period를 사용해 그날 이용 가능했던 값만 선택한다. FOMC 목표 범위는 DFF로 추정하지 않고 공식 statement에서 구조화한다. 공식 결과 metric이 준비되지 않은 이벤트는 가격 반응 분석은 할 수 있지만 결과값에 조건을 건 전략 학습에서는 제외한다.

## 4. Airflow 전체 흐름

### 선택한 방식: 역할별 DAG + 데이터 완료 조건 연결

770개 구간의 수집, 원시 replay, 분석과 백테스트를 하나의 거대한 DAG에 넣지 않는다. 실패 원인과 재실행 범위를 명확히 하기 위해 다음 여섯 DAG로 분리한다.

```text
event_catalog_sync_dag
        ├─→ macro_context_backfill_dag
        └─→ market_context_backfill_dag
                    └─→ raw_replay_validation_dag (대표 구간 또는 부하 실험)

macro context 완료 + market context/coverage 완료
        └─→ event_impact_dag
                    └─→ strategy_backtest_dag
```

- `event_catalog_sync_dag`: 공식 발표 manifest를 검증하고 `economic_events`에 Upsert
- `macro_context_backfill_dag`: 이벤트별 ALFRED vintage와 사전 시장 context 저장
- `market_context_backfill_dag`: 이벤트 × 종목의 1분·일봉 수집, 3분·5분 파생, coverage 저장
- `raw_replay_validation_dag`: 선택한 원시 체결 구간을 Kafka→Spark→DB로 검증. 모든 정기 bar 수집에 강제하지 않음
- `event_impact_dag`: macro와 market의 품질 검사가 모두 통과한 범위만 event study 생성
- `strategy_backtest_dag`: 고정된 데이터 cutoff와 전략 버전으로 백테스트

저장 결과가 다음 DAG의 입력이 되는 지점은 Airflow 3.3 `Asset`으로 연결한다. 단, 단순 task 성공이 아니라 `pipeline_run_checks`의 예상 건수·coverage 조건을 통과한 후에만 asset update를 발생시킨다. 전체 기간을 한 번에 실행할 때에는 별도의 얇은 orchestrator DAG가 위 DAG들을 순서대로 trigger하고 run summary만 모은다. 실제 데이터 행 자체를 XCom으로 넘기지 않는다.

Asset 이름만으로 서로 다른 기간의 실행을 섞지 않는다. 모든 asset event metadata와 `pipeline_runs`에 같은 `pipeline_run_id`, `config_hash`, `data_cutoff`를 기록하고, downstream DAG는 macro와 market check가 같은 세 값을 가진 경우에만 처리한다. Asset은 실행 신호이고 PostgreSQL의 run contract가 정합성 판단 기준이다.

입력값은 `event_types`, `release_from`, `release_to`, `symbols`, `feed`, `analysis_version`, `strategy_version`, `run_mode`다. 코드 수정 없이 기간·이벤트·종목을 바꿀 수 있어야 하며, 모든 실행은 정규화한 입력 JSON과 SHA-256 `config_hash`를 저장한다.

### 작업 분할과 동시 실행 제한

Airflow Dynamic Task Mapping의 기본 재실행 단위는 `event_id + symbol`이다. 단, 770개 API 작업을 무제한으로 동시에 실행하지 않는다. 이벤트 목록을 만든 뒤 event-symbol 목록을 runtime에 map하고 다음 제한을 둔다.

| 자원 | 초기 pool slot | 이유 |
| --- | ---: | --- |
| `alpaca_api_pool` | 2 | rate limit·pagination·네트워크 실패를 제어 |
| `fred_api_pool` | 1 | 동일 이벤트의 10 series를 순차 또는 소규모 묶음으로 요청 |
| `spark_pool` | 1 | 로컬/GCP 한 실행기의 heap·disk 경합 방지 |
| `postgres_write_pool` | 2 | 동시 Upsert와 lock 경합 제한 |

이 값은 영구 정답이 아니라 현재 단일 실행 환경의 안전한 시작값이다. Alpaca 응답의 rate-limit header, 429 횟수, task duration, Spark memory/spill과 DB lock wait를 evidence로 남기고 측정 후 변경한다. DAG는 `max_active_runs=1`, mapped 수집 task는 `max_active_tis_per_dag`를 pool보다 약간 크게 두되 pool이 최종 외부 자원 동시성을 제한한다.

Airflow의 기본 `max_map_length`는 1024이므로 현재 77 × 10 = 770 mapping은 한도 안에 있다. 하지만 향후 이벤트가 103회 이상이 되면 10종목 기준 1,030개로 기본 한도를 넘는다. 따라서 설정값을 무작정 올리지 않고 `release_month` 또는 최대 500개 work item 단위로 run을 나눈다. XCom에는 `event_id`, `symbol`, 저장 경로, checksum, 처리 건수와 상태만 넣고 원시 체결·bar 배열은 넣지 않는다.

한 종목의 API 호출이나 DB 저장이 실패하면 해당 `event_id + symbol`만 재실행한다. 동일 logical date와 입력으로 재실행하면 business key Upsert로 행 수와 결과 hash가 증가하거나 변하지 않아야 한다. 실패한 work item은 `pipeline_run_checks`에 `RETRYABLE`, `TERMINAL` 또는 `DATA_NOT_AVAILABLE` 상태와 함께 기록한다.

## 5. 데이터 결합과 분석

모든 분석은 `economic_event_id`, 공식 `released_at`, `symbol`, `source`, `feed`, `timeframe`, `analysis_version`을 추적한다.

### Event study

각 이벤트·종목에 대해 다음 값을 계산한다.

- 발표 전 60분 수익률과 거래량
- 발표 후 5분·30분·60분·120분 수익률
- 구간 거래량과 realized volatility
- SPY 또는 지정 benchmark 대비 상대수익률
- 이전 1·3·7거래일과 이후 1·3·7거래일 수익률
- 같은 요일·같은 시각의 비발표일 matched baseline
- `COMPLETE`, `PARTIAL`, `NO_MARKET_DATA`, `MARKET_CLOSED`, `FUTURE_SESSION_UNAVAILABLE` coverage

단일 발표 사례의 동시 움직임을 인과효과라고 표현하지 않는다. 이벤트 유형별 반복 횟수, 분포, 중앙값과 coverage를 함께 저장한다.

### 전략 백테스트

첫 전략은 경제지표가 예상보다 높았는지 맞히는 전략이 아니다. 신뢰할 수 있는 과거 전망치가 아직 없으므로, **발표 직후 5분 동안 나타난 시장 반응이 30분까지 이어지는지**를 검증하는 단순한 versioned rule로 고정한다.

```text
대상: COMPLETE coverage가 있는 이벤트·종목
관찰: 발표 직후 0~5분의 종목 수익률 - 같은 구간 SPY 수익률
강도: 발표 후 5분 거래량 / 발표 전 60분을 5분씩 나눈 거래량의 중앙값
신호: 절대 상대수익률이 train의 75분위수 이상이고 거래량 비율이 1.5 이상
진입: 신호 계산이 끝난 다음 1분봉의 시가
방향: 상대수익률과 같은 방향(continuation)
청산: 진입 30분 후에 해당하는 1분봉의 종가
비용: 진입·청산 각각 수수료와 slippage 차감
제약: 이벤트당 종목별 동일 명목, 전체 총 익스포저 상한, 동시 포지션 상한
```

시간 분할은 `2022~2023 train / 2024 validation / 2025~data_cutoff test`로 고정하며 최초 `data_cutoff`는 `2026-08-31`로 둔다. 75분위수와 1.5배 기준은 test 결과를 보기 전에 고정하며 validation 결과로 재조정하지 않는다. 수수료·slippage는 실제 체결비용이라고 주장하지 않고 명시적인 가정으로 저장하며, 왕복 총비용 2bp·6bp·10bp 민감도 결과를 함께 낸다. 발표 시각 이전 데이터, train에서 계산한 75분위수와 현재 bar까지만 사용하도록 feature cutoff를 검사한다.

FOMC는 14:00 ET 정규장 이벤트이고 CPI·고용·PCE는 08:30 ET 장전 이벤트이므로 동일한 진입 규칙을 바로 합치지 않는다. 1차 백테스트는 08:30 이벤트군과 FOMC를 별도 cohort로 계산한다. SPY 자체는 benchmark-relative 신호에서 기준 종목이므로 매매 대상에서 제외하거나 절대수익률 규칙을 별도 버전으로 둔다.

결과에는 누적수익률, 거래 횟수, 승률, 평균·중앙 손익, 최대낙폭, Sharpe, turnover, 비용 전후 수익률, benchmark 대비 결과와 이벤트 유형별 표본 수를 포함한다. 이 전략은 데이터 파이프라인 결과를 실제로 사용하는 첫 검증 규칙이며, 수익을 보장하는 자동매매 전략으로 표현하지 않는다.

표본이 부족한 이벤트 유형은 전략 성과로 확정하지 않고 `INSUFFICIENT_SAMPLE`로 표시한다.

## 6. 저장 모델

기존 테이블을 우선 재사용한다.

- `economic_events`: 공식 이벤트 시각과 출처
- `macro_event_contexts`: 이벤트 당시 알려진 10개 경제·시장 context
- `economic_event_metrics`: 해당 발표에서 실제 공개된 여러 metric과 vintage·원문 출처
- `market_bars`: 1m·3m·5m·1d와 coverage
- `macro_event_impacts`: 이벤트·종목·window별 시장 반응
- `macro_event_baseline_impacts`: matched non-event 비교

백테스트에는 다음 최소 테이블을 추가한다.

- `pipeline_runs`: 입력 JSON, `config_hash`, data cutoff, 코드 버전, 시작·종료 시각과 상태
- `pipeline_work_items`: `event_id + symbol + stage`별 상태, 시도 횟수와 manifest 위치
- `strategy_definitions`: 전략 버전과 변경 불가능한 parameter JSON
- `backtest_runs`: 입력 기간, 데이터 cutoff, 비용 가정, 상태와 결과 요약
- `backtest_trades`: 신호 시각, 체결 가정, 수량, 비용과 손익
- `pipeline_run_checks`: 단계별 예상·처리·저장·오류 건수와 alert 상태

`economic_event_metrics`의 business key는 `(economic_event_id, metric_code, observation_date, realtime_start)`로 한다. 최소 컬럼은 `value`, `unit`, `source`, `source_url`, `retrieved_at`이다. `macro_event_contexts`와 이 테이블을 분리해 “발표 전에 알려진 배경값”과 “발표 순간 새로 공개된 값”을 혼동하지 않는다.

가격 데이터와 백테스트 결과를 같은 테이블에 섞지 않는다.

## 7. 장애·누락 처리

- API 429·5xx: `Retry-After`와 rate-limit header를 우선하고, 제한된 exponential backoff 후 해당 work item 실패 기록
- 잘못된 schema·가격·시간: 저장하지 않고 bounded reason code 기록
- 시장 휴장: `MARKET_CLOSED`, 재시도하지 않음
- 실제 거래 없음: `NO_MARKET_DATA`, provider 비교 후 확정
- 최신 이벤트의 미래 거래일: `FUTURE_SESSION_UNAVAILABLE`, 다음 DAG에서 backfill
- Kafka·Spark 중단: 저장된 offset manifest 또는 checksum이 확인된 Parquet manifest부터 재실행
- DB 실패: transaction rollback 후 같은 파티션 Upsert
- coverage·건수 불일치: 분석과 백테스트를 중단하고 alert 생성

Fallback은 “없던 데이터를 만들어 성공 처리”하는 방식이 아니다. Alpaca API가 최종 실패했을 때 요청 범위·feed·checksum이 일치하는 기존 raw Parquet이 있으면 그 파일로 처리하고 `FALLBACK_USED`를 기록한다. 일치하는 원본이 없으면 fail closed 한다. 경제지표도 검증된 ALFRED vintage 또는 공식 archive가 없으면 최신값으로 대신하지 않고 실패시킨다.

Alert는 최소한 `run_id`, 실패 단계, `event_id`, `symbol`, 예상·실제 건수, 재실행 명령을 포함한다. 과제 제출에서는 `pipeline_run_checks.alert_status='OPEN'`과 Airflow task failure 로그를 실제 동작 증거로 남긴다. 외부 Slack·이메일 전송은 운영 배포 단계로 미루며 구현하지 않은 기능으로 명시한다. 로그에 API key나 DB 비밀번호를 남기지 않는다.

## 8. 검증 전략

### 단위 테스트

- event × symbol 작업 생성
- 시점 기준 ALFRED 값 선택
- 거래일·휴장일·미도래 거래일 판정
- 1분→3분·5분 집계와 coverage
- 수익률·변동성·거래비용 계산
- 미래 데이터 누출 방지

### 통합 테스트

- Airflow DAG 한 이벤트·두 종목 실행
- 수집→파생→DB→impact 전체 경로
- 원시 체결→Kafka→Spark→DB 정합성 대표 파티션
- 과거 재생의 `event_id|symbol|segment` key 순서와 파티션 분포
- 3개·6개 파티션에서 동일 입력 건수·결과 hash와 처리 시간 비교
- 같은 입력 두 번 실행 후 행 수·결과 hash 동일
- API·Spark·DB 실패 후 해당 파티션만 복구

### 전체 실행 증거

- 77회 × 10종목 단계별 처리 건수
- 경제지표 context 예상·저장·결측 건수
- 1m·3m·5m·1d DB 행 수와 coverage
- event impact·baseline·backtest 최종 행 수
- Airflow Grid/Graph와 실패·복구 로그
- Kafka key 변경 전후 파티션별 메시지 분포와 3개·6개 성능 비교
- 백테스트 비용 전후 지표와 데이터 cutoff

## 9. 단계별 완료 조건

### A. 6차시 과제 제출 완료

1. 이미 실행한 기준·부하·장애·복구 수치와 원본 evidence가 서로 일치한다.
2. 최신 구성도와 데이터 모델에 현재 구현과 미구현이 구분돼 있다.
3. Kafka·Spark·PostgreSQL·Airflow의 단계별 건수 확인법이 문서에 있다.
4. 최소 한 개의 실제 fallback 또는 fail-closed 동작과 alert 상태를 재현한다.
5. 77회 × 10종목 bar와 coverage를 Airflow의 parameterized DAG에서 작은 범위로 재실행하고 결과를 증명한다.
6. README와 발표 대본이 실제 결과만 말한다.

### B. 분석·백테스트 완료

다음 항목을 모두 만족해야 이 단계를 완료로 표시한다.

1. 공식 이벤트·경제지표·시장 데이터가 같은 `pipeline_run_id`를 가진 하나의 Airflow orchestrated run으로 연결된다.
2. 2022년 이후로 확장한 전체 공식 event catalog × 10종목의 분석용 bar와 coverage가 재실행 가능하다.
3. 대표 원시 체결 파티션의 Kafka 발행·수신·Spark 입력·DB 결과가 일치한다.
4. 기존 97.5% 파티션 쏠림의 원인이 설명되고 key 변경 후 분포와 처리 시간이 측정된다.
5. 3개·6개 비교 결과에 따라 파티션 수가 근거와 함께 확정된다.
6. 이벤트별 impact와 matched baseline이 생성된다.
7. 미래 정보 없이 재현 가능한 첫 전략 백테스트가 실행된다.
8. 비용 전후 성과와 위험 지표가 저장된다.
9. 누락·휴장·미도래·API 실패·DB 실패가 구분된다.
10. 같은 입력을 재실행해도 최종 행 수와 결과 hash가 변하지 않는다.
11. README, 최신 구성도, 데이터 모델, 실행 증거와 발표 대본이 실제 결과와 일치한다.
12. EMPLOYMENT·PCE·FOMC catalog를 2022년 이후로 확장해 이벤트 유형별 표본 불균형을 줄인다.
13. 08:30 ET 이벤트와 14:00 ET FOMC가 별도 cohort로 검증된다.

이 조건을 충족하기 전에는 Paper Trading이나 실거래 구현을 시작하지 않는다.

## 10. 구현 순서

1. 6차시 제출 문서의 구현·미구현 경계와 alert/fallback 증거 보완
2. 77회 × 10종목 수집기를 `market_context_backfill_dag`에 연결하고 pool·mapping 제한 적용
3. 경제 이벤트 전 유형의 point-in-time context와 실제 발표 metric을 분리 저장
4. EMPLOYMENT·PCE·FOMC 공식 catalog를 2022년 이후로 확장
5. 과거 replay key를 `event_id|symbol|segment`로 변경하고 v2 offset manifest 생성
6. 같은 입력으로 3개·6개 파티션 분포·처리량 비교 후 파티션 수 확정
7. coverage 검사와 누락 work item backfill·alert
8. event impact와 matched baseline을 전체 이벤트로 일반화
9. 백테스트 schema와 고정된 continuation rule 구현
10. point-in-time·비용 민감도 포함 백테스트 실행
11. 전체 실패 복구·멱등성 검증
12. 문서·아키텍처·발표 증거 갱신

## 11. 조사 근거와 설계 판단

- Airflow Dynamic Task Mapping은 실행 시점에 작업 수를 만들며 기본 `max_map_length`는 1024이고, `max_active_tis_per_dag`로 동시에 실행되는 mapped task 수를 제한할 수 있다. 현재 770개는 한도 안이지만 확장 시 run을 나누는 이유다. <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html>
- Airflow Pool은 외부 시스템에 동시에 접근하는 task 수를 제한하기 위한 기능이다. Alpaca·FRED·Spark·PostgreSQL pool을 나누는 근거다. <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html>
- Airflow 3.3은 여러 asset이 모두 갱신된 뒤 downstream DAG를 실행하는 조건을 지원한다. task 성공이 아니라 검증된 dataset 완료를 DAG 경계로 삼는다. <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html>
- Kafka 기본 partitioner는 key가 있으면 key hash로 파티션을 고른다. 기존 `symbol` key가 적은 종목 수와 거래량 편차 때문에 쏠린 원인이며, producer idempotence와 ordering 설정도 함께 검증한다. <https://kafka.apache.org/42/generated/producer_config.html>
- Spark Structured Streaming checkpoint에는 처리 offset과 집계 상태가 저장되고 실패 후 그 위치에서 복구한다. query·topic 계약이 바뀐 v2가 v1 checkpoint를 재사용하지 않는 이유다. <https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html>
- Alpaca historical trades는 한 페이지에 최대 10,000건이며 더 많은 데이터는 `next_page_token`을 따라가야 한다. Basic historical API 한도는 분당 200회이므로 무제한 mapping 대신 pool과 pagination manifest를 사용한다. <https://docs.alpaca.markets/us/reference/stocktrades-1>, <https://docs.alpaca.markets/us/docs/about-market-data-api>
- ALFRED/FRED observations의 `vintage_dates`는 지정한 과거 날짜 당시 존재했던 데이터를 내려준다. 최신 수정값이 과거 백테스트에 섞이지 않게 하는 근거다. <https://fred.stlouisfed.org/docs/api/fred/series_observations.html>
- CPI·고용·PCE·FOMC의 날짜·시각과 실제 공개값은 각 기관의 공식 archive를 기준으로 확장한다. <https://www.bls.gov/cpi/news.htm>, <https://www.bls.gov/bls/news-release/empsit.htm>, <https://www.bea.gov/news/schedule>, <https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm>
