# Airflow market replay 실행 증거

## 검증 조건

- DAG: `market_sip_replay_pipeline`
- 실행일: 2026-08-27
- 데이터 출처: Alpaca Historical Trades API
- feed: `sip`
- 조회 범위: `2026-08-12T12:25:00Z` 이상, `2026-08-12T12:35:00Z` 미만
- 실행 A 입력: `tickers=[SPY, QQQ, SMH, NVDA]`
- 실행 B 입력: 코드를 고치지 않고 `tickers=[SPY, QQQ]`로 목록 변경
- 경로: `Alpaca → Kafka raw.market-sip.v1 → Spark → PostgreSQL market_bars`

## 결과

### 실행 A — 네 종목 한 번에 입력

- run ID: `manual__2026-08-27T07:30:57.734232+00:00`
- DAG 최종 상태: `success`

| 항목 | SPY | QQQ | SMH | NVDA | 합계 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Alpaca 수집 체결 | 3,307 | 6,143 | 931 | 4,688 | 15,069 |
| Kafka Producer 발행 | 3,307 | 6,143 | 931 | 4,688 | 15,069 |
| Kafka Consumer 수신 | 3,307 | 6,143 | 931 | 4,688 | 15,069 |
| Spark 입력 | 3,307 | 6,143 | 931 | 4,688 | 15,069 |
| Spark validation 오류 | 0 | 0 | 0 | 0 | 0 |
| Spark 중복 제거 대상 | 0 | 0 | 0 | 0 | 0 |
| 가격·VWAP 반영 체결 | 1,788 | 2,561 | 315 | 824 | 5,488 |
| 생성·Upsert 1분봉 | 10 | 10 | 10 | 10 | 40 |
| PostgreSQL 검증 행 | 10 | 10 | 10 | 10 | 40 |
| mapped task 상태 | success | success | success | success | success |

실행 A 한 번에서 validation 1개와 종목별 mapped task 16개가 생성됐다. `map_index 0·1·2·3`은 입력 순서대로 `SPY·QQQ·SMH·NVDA`에 대응하며 모두 성공했다.

### 실행 B — 입력 목록 변경

- run ID: `manual__2026-08-27T07:32:54.583401+00:00`
- 변경 입력: `tickers=[SPY, QQQ]`
- DAG 최종 상태: `success`
- validation 1개와 mapped task 8개 모두 `success`

Producer가 확인한 Kafka offset 범위를 Consumer와 Spark에 전달해 각 mapped task가 쓴 범위만 읽었다. 실행 A에서 SMH는 partition 2의 `[0, 931)`, NVDA는 partition 1의 `[78714, 83402)`, SPY는 partition 1의 `[83402, 86709)`, QQQ는 partition 1의 `[86709, 92852)`를 처리했다. 공통 토픽을 사용하지만 trace ID와 offset 범위를 함께 사용하므로 종목과 실행이 섞이지 않는다.

## 제출 파일

- [`multi-symbol-run-summary.json`](multi-symbol-run-summary.json): 실행 A·B 입력, run ID와 단계별 실제 건수
- [`multi-symbol-task-states.txt`](multi-symbol-task-states.txt): 두 DAG run의 mapped task 상태
- [`postgres-result.txt`](postgres-result.txt): PostgreSQL에서 다시 조회한 네 종목 집계와 실제 1분봉 샘플 8행

Airflow 전체 로그와 메타데이터 DB는 `airflow-runtime/` 아래에 있으며 Git에서 제외한다. 이 폴더에는 API key, DB 비밀번호, 원시 거래 payload가 없는 작은 결과만 포함한다.

## PostgreSQL 교차 확인

```text
symbol | bars | first_bar              | last_bar               | trade_count_sum
NVDA   | 10   | 2026-08-12 12:25:00+00 | 2026-08-12 12:34:00+00 | 4688
QQQ    | 10   | 2026-08-12 12:25:00+00 | 2026-08-12 12:34:00+00 | 6143
SMH    | 10   | 2026-08-12 12:25:00+00 | 2026-08-12 12:34:00+00 | 931
SPY    | 10   | 2026-08-12 12:25:00+00 | 2026-08-12 12:34:00+00 | 3307
```

`trade_count_sum`은 종목별로 저장된 열 개 1분봉의 `trade_count` 합계다. 원시 거래 파일이나 개별 체결 payload를 저장소에 복사한 값이 아니다.

동일한 조회는 [`airflow_market_replay_results.sql`](../../../scripts/evidence/airflow_market_replay_results.sql)로 재현할 수 있다.

```bash
docker compose exec -T postgres psql -U market -d market -f - \
  < scripts/evidence/airflow_market_replay_results.sql
```
