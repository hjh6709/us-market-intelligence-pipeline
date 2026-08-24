# CPI 발표 구간 SIP Kafka·Spark 전처리·저장 결과

- 검증일: 2026-08-24 KST
- 경제 이벤트: 2026-08-12 CPI 발표, `08:30 ET(12:30 UTC)`
- 데이터: Alpaca Historical Trades API의 실제 NVDA SIP 체결
- 대상 구간: `11:30 UTC` 이상, `13:31 UTC` 미만
- Topic: `raw.market-sip.v1`
- trace: `cpi-20260812-nvda-sip-001`

## Producer·Consumer

| 항목 | 결과 |
| --- | ---: |
| Alpaca SIP 원시 체결 조회 | 58,036건, 6페이지 |
| Kafka 발행 | 58,036건 |
| Consumer 수신 | 58,036건 |
| 발행·수신 차이 | 0건 |

첫 체결은 `11:30:02.284268354 UTC`, 마지막 체결은 `13:30:59.983248061 UTC`다. 발표 전 60분부터 발표 후 60분까지의 121개 1분 구간에 실제 데이터가 모두 존재한다.

## Spark 전처리·저장

| 처리 단계 | 결과 |
| --- | ---: |
| Kafka source 입력 | 58,036건 |
| validation 오류 | 0건 |
| `event_id` 중복 제거 후 | 58,036건 |
| Spark 1분 OHLCV 출력 | 121건 |
| PostgreSQL upsert | 121건 |
| 중복 business key | 0건 |

Spark batch는 Kafka의 동일 trace를 읽어 JSON schema parsing, 필수 필드·종목·가격·수량·UTC timestamp 검증, `event_id` 중복 제거, event-time 1분 OHLCV·거래 건수·VWAP 집계를 수행했다. 결과는 기존 Alpaca provider bar를 덮어쓰지 않도록 `source=alpaca_replay`, `feed=sip`로 저장했다.

## provider bar와 raw replay 비교

| 결과 | 1분봉 | 거래 건수 합계 |
| --- | ---: | ---: |
| Alpaca provider SIP bar | 121 | 58,034 |
| SIP raw trade Spark 재구성 | 121 | 58,036 |

두 건 차이는 숨기거나 임의 보정하지 않는다. provider bar의 `trade_count`는 Alpaca의 봉 생성 정책 결과이고, replay 값은 Historical Trades API에서 받은 모든 원시 행을 현재 프로젝트 규칙으로 집계한 결과다. 이후 거래 조건 코드 정책을 명시하고 두 건의 포함 여부를 분석한다.

기계 판독용 공개 집계는 [result.json](../evidence/cpi-kafka-spark/result.json)에 있다. 멘토 앞에서 재확인하는 명령은 [실행 증거 안내](../evidence/cpi-kafka-spark/README.md)에 정리했다. API key, 원시 체결 payload, 정확한 가격 행과 로컬 DB는 Git에 포함하지 않는다.
