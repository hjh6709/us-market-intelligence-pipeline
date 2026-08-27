# Airflow market replay 실행 증거

## 검증 조건

- DAG: `market_sip_replay_pipeline`
- 실행일: 2026-08-27
- 데이터 출처: Alpaca Historical Trades API
- feed: `sip`
- 조회 범위: `2026-08-12T12:25:00Z` 이상, `2026-08-12T12:35:00Z` 미만
- 변경한 입력값: 두 번째 실행에서 `ticker`만 `NVDA`에서 `SPY`로 변경
- 경로: `Alpaca → Kafka raw.market-sip.v1 → Spark → PostgreSQL market_bars`

## 결과

| 항목 | NVDA | SPY |
| --- | ---: | ---: |
| Alpaca 수집 체결 | 4,688 | 3,307 |
| Kafka Producer 발행 | 4,688 | 3,307 |
| Kafka Consumer 수신 | 4,688 | 3,307 |
| Spark 입력 | 4,688 | 3,307 |
| Spark validation 오류 | 0 | 0 |
| Spark 중복 제거 대상 | 0 | 0 |
| 가격·VWAP 반영 체결 | 824 | 1,788 |
| 생성·Upsert 1분봉 | 10 | 10 |
| PostgreSQL 검증 행 | 10 | 10 |
| DAG 최종 상태 | success | success |

Producer가 확인한 Kafka offset 범위를 Consumer와 Spark에 전달해 각 실행이 쓴 범위만 읽었다. NVDA는 offset `70719` 이상 `75407` 미만의 4,688건, SPY는 `75407` 이상 `78714` 미만의 3,307건이다. 따라서 두 번째 SPY 실행은 앞선 NVDA 메시지를 다시 훑지 않았고 `scanned_messages`도 3,307건으로 수신 건수와 같다.

## 제출 파일

- [`nvda-run-summary.json`](nvda-run-summary.json): NVDA 실행 입력과 단계별 실제 건수
- [`spy-run-summary.json`](spy-run-summary.json): ticker만 변경한 SPY 실행 입력과 단계별 실제 건수
- [`nvda-airflow-log.txt`](nvda-airflow-log.txt): NVDA의 정제된 Airflow task 로그
- [`spy-airflow-log.txt`](spy-airflow-log.txt): SPY의 정제된 Airflow task 로그

Airflow 전체 로그와 메타데이터 DB는 `airflow-runtime/` 아래에 있으며 Git에서 제외한다. 이 폴더에는 API key, DB 비밀번호, 원시 거래 payload가 없는 작은 결과만 포함한다.

## PostgreSQL 교차 확인

```text
symbol | bars | first_bar              | last_bar               | trade_count_sum
NVDA   | 10   | 2026-08-12 12:25:00+00 | 2026-08-12 12:34:00+00 | 4688
SPY    | 10   | 2026-08-12 12:25:00+00 | 2026-08-12 12:34:00+00 | 3307
```

`trade_count_sum`은 저장된 열 개 1분봉의 `trade_count` 합계다. 원시 거래 파일이나 개별 체결 payload를 저장소에 복사한 값이 아니다.
