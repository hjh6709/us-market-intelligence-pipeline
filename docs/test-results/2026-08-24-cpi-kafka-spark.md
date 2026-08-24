# CPI 발표 구간 SIP Kafka·Spark 전처리·저장 결과

- 검증일: 2026-08-24 KST
- 경제 이벤트: 2026년 7월 미국 CPI, 2026-08-12 `08:30 ET(12:30 UTC)` 발표
- 데이터: Alpaca Historical Trades API의 실제 NVDA 개별 체결 레코드
- 대상 구간: `[07:30, 09:31) ET` = `[11:30, 13:31) UTC`
- Topic: `raw.market-sip.v1`
- trace: `cpi-20260812-nvda-sip-001`

## 데이터 계약

| 항목 | 정의 |
| --- | --- |
| 조회 조건 | `symbol=NVDA`, `feed=sip`, `start=2026-08-12T11:30:00Z`, `end=2026-08-12T13:31:00Z` |
| 시간 조건 | 1분 버킷 `T-60`부터 `T+60`까지 포함하도록 조회한 반개구간 `[start, end)`. 정규장 첫 1분 포함 |
| 원본 grain | Alpaca가 반환한 개별 체결 레코드 한 행 |
| 레코드 주요 값 | 거래 ID, 거래소, 가격, 수량, 조건, 체결 시각, 테이프 코드 |
| 58,036의 의미 | 위 조회 조건으로 반환된 행의 수. 거래량·하루치·전체 종목·CPI 지표 건수가 아님 |
| Kafka key | `symbol=NVDA` |
| 중복 식별 | source·feed·symbol·provider trade ID·event timestamp 기반 `event_id` |
| 처리 결과 grain | symbol·event-time 1분별 OHLCV 한 행 |

## Producer·Consumer

| 항목 | 결과 |
| --- | ---: |
| NVDA Historical SIP 체결 레코드 조회 | 58,036건, 6페이지 |
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
| 지원하지 않는 거래 조건 | 0건 |
| volume·trade_count 반영 | 58,034건 |
| OHLC·VWAP 가격 형성 반영 | 8,752건 |
| Spark 1분 OHLCV 출력 | 121건 |
| PostgreSQL upsert | 121건 |
| 중복 business key | 0건 |

Spark batch는 Kafka의 동일 trace를 읽어 JSON schema parsing, 필수 필드·종목·가격·수량·UTC timestamp 검증, `event_id` 중복 제거 후 Alpaca의 CTA/UTP sale-condition 규칙을 적용했다. 거래 조건에 따라 OHLC 가격 형성과 volume·trade_count 반영 여부를 분리하고, 여러 조건이 있으면 가장 엄격한 규칙을 적용한다. 그 뒤 event-time 1분 OHLCV·거래 건수·VWAP를 집계했다. 결과는 기존 Alpaca provider bar를 덮어쓰지 않도록 `source=alpaca_replay`, `feed=sip`로 저장했다.

## provider bar와 raw replay 비교

| 결과 | 정확한 데이터 정의 | 1분봉 | 거래 건수 합계 |
| --- | --- | ---: | ---: |
| Alpaca provider SIP bar | `NVDA`, `feed=sip`, `timeframe=1Min`, `[2026-08-12 11:30, 13:31) UTC`로 받은 bar의 `SUM(trade_count)` | 121 | 58,034 |
| SIP raw trade Spark 재구성 | 동일 종목·feed·구간의 Historical Trades API 원시 행에 `alpaca_sip_minute_v1`을 적용한 `SUM(trade_count)` | 121 | 58,034 |

58,036개의 raw trade 중 `Q(Official Open)` 조건을 포함한 2건은 provider 규칙상 minute bar의 가격·volume·trade_count를 갱신하지 않아 재구성 합계도 58,034건이 됐다. 가격 형성에는 8,752건만 사용되고, 가격에서 제외되더라도 volume에는 포함되는 거래가 존재한다. provider와 replay 121개 bar를 행별 비교한 결과 OHLC·volume·trade_count·VWAP 불일치는 각각 0건이었다. 따라서 숫자를 강제로 맞춘 것이 아니라 공식 집계 규칙을 코드로 구현해 동일 결과를 재현했다.

가격 형성에서 제외되고 volume에는 반영된 거래는 49,282건이다. 그중 대부분은 Odd Lot(`I`) 조건을 포함했다. 주요 조합은 `[@, T, I]` 15,417건, `[@, F, T, I]` 11,887건, `[@, I]` 10,667건, `[@, 4, I]` 7,734건, `[@, F, I]` 3,422건이다. 이는 데이터 손실이 아니라 특수 조건 거래로 OHLC/VWAP가 왜곡되는 것을 막으면서 실제 거래량은 보존하는 provider-compatible 전처리다.

기계 판독용 공개 집계는 [result.json](../evidence/cpi-kafka-spark/result.json)에 있다. 멘토 앞에서 재확인하는 명령은 [실행 증거 안내](../evidence/cpi-kafka-spark/README.md)에 정리했다. API key, 원시 체결 payload, 정확한 가격 행과 로컬 DB는 Git에 포함하지 않는다.
