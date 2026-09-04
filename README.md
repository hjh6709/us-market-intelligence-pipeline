# U.S. Economic Event Market Reaction Pipeline

미국의 CPI·고용보고서·PCE·FOMC 발표 시각과 시장 반응을 같은 시간축으로 연결하는 데이터 파이프라인입니다. 수집·전처리·저장·재실행·품질 확인뿐 아니라 저장 결과를 읽는 FastAPI와 웹 대시보드까지 연결했습니다. 최종 목표는 자동매매이지만 현재 전략 성과와 주문 안전장치가 준비되지 않았으므로 운영 단계는 `RESEARCH_ONLY`, 실제 행동은 `NO_TRADE`입니다.

![전체 프로젝트 데이터 파이프라인 아키텍처](docs/diagrams/pipeline-architecture.png)

## 30초 요약

현재는 목적이 다른 다섯 경로가 실제로 동작합니다.

1. **원시 체결 검증:** 과거 SIP 개별 체결을 Parquet에 보관하고 Kafka로 재생해 Spark가 1분봉을 만듭니다.
2. **시장 반응 데이터:** 공식 발표 202회와 10종목을 기준으로 Alpaca SIP 1분봉·일봉을 수집하고 3분봉·5분봉을 만듭니다.
3. **경제 상황 데이터:** 각 발표 시점에 이용 가능했던 FRED·ALFRED 10개 지표를 PostgreSQL에 연결합니다.
4. **분석·기준 전략:** 발표 전후 5·30·60분 반응을 계산하고, 발표 전 가격 방향만 사용하는 탐색 전략을 비용 포함으로 검증합니다.
5. **서빙:** PostgreSQL의 최종 결과를 읽기 전용 JSON API와 `Macro Pulse` 대시보드에서 조회합니다.

이번 확장 실행 결과는 다음과 같습니다.

| 항목 | 실제 결과 |
| --- | ---: |
| 공식 발표 | CPI 55 + 고용 55 + PCE 55 + FOMC 37 = **202회** |
| 분석 종목 | **10종목** |
| 발표-종목 구간 | **2,020개** |
| 발표 T-60~T+120분의 실제 1분봉 선택 합계 | **308,512행** |
| 1분봉에서 만든 3분봉 / 5분봉 | **112,593 / 70,090행** |
| 발표 전후 7거래일 일봉 선택 합계 | **30,250행** |
| Kafka v2 검증 | **118,118건 발행 = 수신 = Spark 입력** |
| Kafka 최대 파티션 비중 | 기존 97.5% → v2 **33.9%** |
| 발표 시점 경제 맥락 | 202회 × 10 series = **2,020행** |
| Airflow 전체 실행 | 시장 202 tasks·522.660초 / 거시 202 tasks·14.835초 |
| 이벤트 구간 지표 | 202회 × 10종목 × 4구간 = **8,080행** |
| 탐색 전략 | 실행 가능 1,988행, 비용 차감 평균 **-0.1565%** |
| 단일 서빙 시연 | CPI 1회 × NVDA 1종목, 처리·저장·재조회 **0.30초** |
| 자동매매 준비 상태 | **RESEARCH_ONLY / NO_TRADE** |

`선택 합계`는 각 발표를 기준으로 조회한 행을 더한 값입니다. 인접한 발표가 같은 시장 시각이나 거래일을 공유할 수 있으므로 PostgreSQL은 동일한 business key를 한 번만 저장합니다. 따라서 테이블의 고유 행 수와 선택 합계는 서로 다른 지표입니다.

## 이번 제출부터 확인하기

1. [7차시 서빙 레이어 제출 문서](docs/serving-layer-assignment.md): 저장 결과 조회, 단일 실행, 자동매매 경계
2. [7차시 실제 실행 증거](docs/evidence/serving-layer/README.md): JSON 응답, 멱등 실행, 대시보드 캡처
3. [7차시 4분 발표 대본](docs/09.07_대본.md)
4. [6차시 부하·복구 제출 문서](docs/load-recovery-assignment.md)
5. [전체 확장 증거](docs/evidence/multi-event-expansion/README.md)

## 프로젝트 목표

- 공식 기관 발표 시각을 기준 이벤트로 보존합니다.
- 발표 당시 알 수 있었던 경제지표 값만 연결해 미래 정보 혼입을 줄입니다.
- 시장 데이터의 종목·feed·시간 범위와 결측 사유를 기록합니다.
- Kafka와 Spark로 원시 체결의 전달·검증·1분 집계를 재현합니다.
- 같은 입력을 다시 실행해도 PostgreSQL에 중복 저장되지 않게 합니다.
- 분석 결과와 성과가 없었던 기준 전략도 재현 가능한 결과로 보존합니다.
- 저장 결과를 API와 대시보드로 제공하고, 연구 신호와 실제 주문 행동을 분리합니다.

## 현재 분석 범위

| 구분 | 범위 |
| --- | --- |
| 공식 발표 | 2022-01-07~2026-08-26의 CPI 55회, Employment 55회, PCE 55회, FOMC 37회 |
| 종목 | `SPY`, `QQQ`, `IWM`, `TLT`, `XLF`, `SMH`, `GLD`, `NVDA`, `AAPL`, `JPM` |
| 장중 구간 | 발표 60분 전부터 120분 후까지, 최대 181개 1분 구간 |
| 일별 구간 | 발표일 이전 7거래일 + 발표일 + 이후 7거래일 |
| 파생 해상도 | 실제 1분봉을 묶은 3분봉·5분봉과 `COMPLETE/PARTIAL` coverage |
| 경제 맥락 | FRED·ALFRED 10개 series의 발표 시점 기준 값 |

8시 30분 발표는 장전 거래를 포함하므로 유동성이 낮은 종목은 181개 분이 모두 존재하지 않을 수 있습니다. 없는 가격을 임의로 채우지 않습니다. 3분봉과 5분봉은 결측을 숨기는 대체 데이터가 아니라, 포함된 1분봉 수를 함께 기록하는 별도 해상도입니다.

## 데이터 흐름

```text
A. 원시 체결 정확성 검증
Alpaca SIP trades → Parquet → Kafka v2 → Spark → PostgreSQL market_bars

B. 분석용 시장 데이터
공식 발표 목록 → Airflow → Alpaca 1m·1d → 3m·5m 생성 → PostgreSQL

C. 발표 시점 경제 상황
공식 발표 목록 → Airflow → FRED·ALFRED → PostgreSQL macro_event_contexts

D. 이벤트 분석
market_bars + economic_events → 구간 수익률·거래량·변동성 → 탐색용 비용 포함 backtest

E. 읽기 전용 서빙
PostgreSQL → ServingService → FastAPI JSON API + Macro Pulse Dashboard
```

원시 체결 경로와 분석용 bar 경로는 행의 의미가 다릅니다. `7,360,804건`은 CPI 55회 × 4종목의 **개별 체결** 부하 입력이고, `308,512행`은 202회 × 10종목의 이벤트별 **1분봉 선택 합계**입니다. 두 숫자를 더하거나 직접 비교하지 않습니다.

## 데이터 출처

| 데이터 | 출처 | 역할 |
| --- | --- | --- |
| CPI 일정 | [BLS CPI](https://www.bls.gov/bls/news-release/cpi.htm) | 공식 발표일·시각 |
| 고용보고서 일정 | [BLS Employment Situation](https://www.bls.gov/bls/news-release/empsit.htm) | 공식 발표일·시각 |
| PCE 일정 | [BEA Personal Income and Outlays](https://www.bea.gov/news/archive?field_related_product_target_id=476) | 공식 발표일·시각 |
| FOMC 일정 | [Federal Reserve FOMC calendars](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm) | statement 발표일·시각 |
| 경제지표 값 | [FRED/ALFRED](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) | 발표 당시 이용 가능한 경제 상황 |
| 시장 데이터 | [Alpaca Historical Stock Data](https://docs.alpaca.markets/reference/stockbars) | SIP 개별 체결·1분봉·일봉 |

## 실제 구현 결과

### A. Kafka·Spark 원시 체결 처리

기준 실행은 2026-08-12 CPI 발표 구간의 네 종목입니다.

| 단계 | 결과 |
| --- | ---: |
| Parquet 원시 체결 | 118,118 |
| Kafka 발행 / 수신 | 118,118 / 118,118 |
| Spark 입력 / 형식 오류 / 실제 중복 | 118,118 / 0 / 0 |
| Spark 생성 1분봉 | 472 |
| PostgreSQL business key 중복 | 0 |

Kafka v1은 `symbol`만 key로 사용해 네 종목 중 거래량이 큰 종목이 있는 파티션에 97.5%가 몰렸습니다. v2는 `event type + 발표일 + symbol + 15분 segment`를 key로 사용하고 파티션을 6개로 조정했습니다. 동일한 118,118건을 실제 재실행한 결과 가장 큰 파티션의 비중이 33.9%로 낮아졌습니다. 완전히 균등하다고 주장하지는 않습니다.

Spark는 JSON 형식·필수값·가격·수량을 검사하고, 거래소를 포함한 결정적 `event_id`로 중복을 판별합니다. 거래 조건을 적용해 event time 기준 1분 OHLCV·거래 건수·VWAP을 만든 뒤 PostgreSQL에 Upsert합니다.

### B. 202개 발표·10종목 시장 데이터

Alpaca 다종목 Bars API를 사용해 한 발표마다 1분봉과 일봉을 각각 한 번 요청합니다. 종목별 요청 방식의 4,040회보다 적은 **404회(페이지 추가 전 기준)**로 2,020개 발표-종목 구간을 처리했습니다.

| 데이터 | 이벤트별 선택·생성 합계 | 설명 |
| --- | ---: | --- |
| SIP 1분봉 | 308,512 | 발표 T-60~T+120분에 실제 존재한 봉 |
| 3분봉 | 112,593 | 그중 PARTIAL 19,178 |
| 5분봉 | 70,090 | 그중 PARTIAL 16,215 |
| SIP 일봉 | 30,250 | 발표 전후 7거래일을 이벤트별로 선택한 합계 |

일봉 coverage 2,020건 중 1,980건은 완전하고 40건은 이유가 확인된 불완전 구간입니다.

- 2023-04-07 Employment, 2024-03-29 PCE, 2026-04-03 Employment는 Good Friday 휴장이라 각각 10종목의 발표일 일봉이 없습니다.
- 2026-08-26 PCE는 실행 기준일에 이후 거래일이 5일만 존재해 10종목 모두 미래 2거래일을 만들지 않았습니다.

### C. Airflow 자동화

`market_context_backfill_pipeline`은 event type, 날짜 범위, 종목 목록, feed, 데이터 기준시각을 입력받습니다. **경제발표 한 건을 Airflow task 하나로 만들고 그 안에서 10종목을 묶어 조회**합니다. DB에는 종목별 work item과 품질검사를 따로 기록하므로 실패 범위를 확인할 수 있습니다.

FOMC 2026-07-29의 `SPY`, `TLT` smoke 후, 공식 발표 202회 전체를 실제 실행했습니다. 시장 DAG의 mapped task 202개는 종목별 work item 2,020개를 522.660초에 처리했고, 1,980개 성공과 이유가 확인된 40개 미제공 상태를 모두 수용했습니다. 실패와 미해결 alert는 0개였습니다.

`macro_context_backfill_pipeline`은 발표별 FRED·ALFRED 값을 수집합니다. 외부 API 호출량을 제한하기 위해 `fred_api_pool`을 사용합니다. 전체 CLI 실행에서는 CPI 550행, 고용 550행, PCE 550행, FOMC 370행으로 총 2,020개 context를 저장했습니다. Airflow 전체 실행에서는 이미 검증된 2,020개를 재호출하지 않는 멱등 모드로 202개 mapped task와 최종 검증 task를 14.835초에 완료했습니다.

### D. 이벤트 분석과 탐색용 기준 전략

공식 발표 시각을 기준으로 각 종목의 발표 전 60분과 발표 후 5·30·60분 수익률, 거래량, 변동성, SPY 대비 수익률을 계산해 8,080행을 저장했습니다.

전망치·surprise가 없는 상태에서 미래 정보를 쓰지 않기 위해, 발표 전 60분 수익률이 양수면 long, 음수면 short로 진입해 발표 60분 후 청산하는 단순 기준만 실행했습니다. 왕복 비용 10bp를 차감한 1,988개 실행 가능 결과의 평균은 -0.1565%, 중앙값은 -0.1251%, 양수 비율은 39.34%였습니다. 이는 수익 전략이 아니라 현재 규칙이 작동하지 않았다는 검증 결과입니다. 여러 종목을 합친 포트폴리오 성과나 예상 수익률로 해석하지 않습니다.

## 실행 방법

### 1. 환경 준비

```bash
cp .env.example .env
uv sync --extra airflow
docker compose up -d --wait postgres kafka kafka-init
```

`.env`에는 `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, `FRED_API_KEY`를 입력합니다. 비밀키·원본 API 응답·대용량 Parquet은 Git에 올리지 않습니다.

### 2. 전체 분석용 시장 데이터

```bash
# API 호출 없이 작업 수 확인
.venv/bin/python scripts/collect_market_event_context.py --dry-run

# 202개 발표 × 10종목 실행
.venv/bin/python scripts/collect_market_event_context.py \
  --event-types CPI EMPLOYMENT PCE FOMC \
  --release-from 2022-01-01 --release-to 2026-08-26 \
  --symbols SPY QQQ IWM TLT XLF SMH GLD NVDA AAPL JPM \
  --feed sip
```

### 3. 발표 시점 경제 맥락

```bash
.venv/bin/python scripts/collect_macro_event_context.py \
  --event-types CPI EMPLOYMENT PCE FOMC \
  --release-from 2022-01-01 --release-to 2026-08-26
```

### 4. Airflow

```bash
export AIRFLOW_HOME="$PWD/airflow-runtime"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

.venv/bin/airflow db migrate
.venv/bin/python scripts/configure_airflow_pools.py
.venv/bin/airflow dags test market_context_backfill_pipeline \
  -f "$PWD/dags/market_context_backfill_pipeline.py" \
  -c '{"event_types":["FOMC"],"release_from":"2026-07-29","release_to":"2026-07-29","symbols":["SPY","TLT"],"feed":"sip","data_cutoff":"2026-09-03T00:00:00Z"}'
```

다년 실행은 `market_context_backfill_orchestrator`가 연도별 child run으로 나눕니다. 각 child DAG는 발표별 task를 만들고, 실패한 연도나 발표 범위만 다시 실행할 수 있습니다.

### 5. 저장 결과를 읽는 API와 대시보드

외부 API나 증권사 주문 API 없이 로컬 PostgreSQL의 저장 결과만 읽습니다.

```bash
.venv/bin/uvicorn src.serving_api:app --host 127.0.0.1 --port 8000
```

브라우저에서 `http://127.0.0.1:8000/`을 열면 발표·종목을 선택해 1분·3분·5분봉, 발표 전후 반응, 경제 환경, 전략 시뮬레이션과 자동매매 준비 상태를 확인할 수 있습니다. JSON API 명세는 `http://127.0.0.1:8000/docs`에서 봅니다.

발표용 입력 → 처리 → 저장 → 읽기 시연은 다음 한 명령으로 끝납니다.

```bash
.venv/bin/python scripts/run_serving_demo.py \
  --event-id 'CPI|2026-07|2026-08-12T12:30:00Z' \
  --symbol NVDA
```

실제 측정 시간은 0.30초였고, 같은 입력을 반복해도 영향 고유키 4개와 전략 고유키 1개가 유지됐습니다.

### 6. 검증

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python scripts/evidence/export_multi_event_summary.py

# 이벤트 구간 8,080행과 탐색 전략 2,020행 생성
.venv/bin/python -m src.macro_event_impact
.venv/bin/python -m src.event_strategy_backtest
.venv/bin/python -m scripts.evidence.export_event_analysis
```

## 저장 모델

| 테이블 | 한 행의 의미 | business key |
| --- | --- | --- |
| `economic_events` | 공식 경제 발표 한 번 | event type·reference period·released at |
| `macro_event_contexts` | 발표 시점에 이용 가능했던 지표 하나 | event ID·series ID |
| `market_bars` | 종목의 1m·3m·5m·1d 봉 하나 | symbol·start·timeframe·source·feed |
| `pipeline_runs` | 파이프라인 실행 한 번 | pipeline run ID |
| `pipeline_work_items` | 실행 안의 event·symbol·stage 작업 | run·event·symbol·stage |
| `pipeline_run_checks` | 품질검사와 alert 상태 | run·event·symbol·stage·check |
| `macro_event_impacts` | 발표·종목·구간별 시장 반응 | event·symbol·window·analysis version |
| `event_strategy_results` | 발표·종목별 탐색 전략 결과 | event·symbol·strategy·version |

## 다음 단계

- 발표별 실제값·시장 전망치·surprise를 신뢰할 수 있는 point-in-time 출처로 추가
- 비발표일 비교군과 다른 사건을 통제한 통계 검정
- 호가 기반 슬리피지·포트폴리오 제약을 반영한 전략 검증
- 검증된 archive fallback과 운영 알림 채널 연결
- `RESEARCH_ONLY` 다음에 Alpaca 모의주문과 주문·부분 체결·포지션 복구 검증
- 최대 주문 금액·보유 종목 수·일일 손실 한도·중복 주문 방지·긴급 중지 구현
- Slack 사람 승인 단계를 거친 뒤 소액 `LIMITED_LIVE`, 충분한 운영 검증 후 `AUTOMATED_LIVE` 검토

## 구현·과제 증거

- [3차시 Kafka·Spark 과제](docs/kafka-spark-assignment.md)
- [4차시 Airflow 과제](docs/airflow-assignment.md)
- [5차시 부하·장애·복구 과제](docs/load-recovery-assignment.md)
- [식별키 수정 후 전체 재실행](docs/pipeline-review-assignment.md)
- [다중 경제 이벤트 확장](docs/multi-event-expansion.md)
- [7차시 서빙 레이어와 최종 발표](docs/serving-layer-assignment.md)
- [7차시 실제 API·대시보드 증거](docs/evidence/serving-layer/README.md)

## 면책 및 출처 고지

교육·연구용 프로젝트이며 투자 조언이 아닙니다. 현재 계좌·주문 API를 호출하지 않으며 대시보드의 `LONG/SHORT`는 과거 분석 신호일 뿐 주문이 아닙니다.

This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.
