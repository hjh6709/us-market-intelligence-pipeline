# CPI 발표 구간 Kafka·Spark 전처리·저장 결과

- 검증일: 2026-08-24 KST
- 경제 이벤트: 2026-08-12 CPI 발표, `08:30 ET(12:30 UTC)`
- 데이터: Alpaca Historical Trades API의 실제 NVDA IEX 체결
- 요청 구간: `11:30 UTC` 이상, `13:34 UTC` 미만
- Topic: `raw.market.v1`
- trace: `cpi-20260812-nvda-001`

이 실행은 임의 장중 구간이 아니라 현재 프로젝트에서 분석하는 실제 CPI 발표 시각과 동일한 구간을 사용한다. 발표 전 60분부터 발표 60분 후 정규장 개장 시점, 그리고 개장 후 4분까지 포함한다. 개장 구간은 실제 개장 반응을 포함하면서 Spark의 event-time watermark를 진행시키는 역할도 한다.

## Producer·Consumer

| 항목 | 결과 |
| --- | ---: |
| Alpaca 조회 | 1,576건 |
| Kafka 발행 | 1,576건 |
| Consumer 수신 | 1,576건 |
| 발행·수신 차이 | 0건 |

무료 IEX feed에서 요청 구간의 첫 실제 체결은 `12:10 UTC`였다. 따라서 발표 전 60분 전체에 체결이 존재했다고 주장하지 않으며, 이 coverage 한계는 Historical SIP 1분봉과 별도로 비교한다.

## Spark 전처리·저장

| 처리 단계 | 결과 |
| --- | ---: |
| Kafka source 입력 | 1,576건 |
| validation 오류 | 0건 |
| 확정 bar 반영 거래 | 509건 |
| 실행 종료 시 미확정 차이 | 1,067건 |
| PostgreSQL 최종 1분봉 | 18건 |
| 중복 business key | 0건 |
| 확정 bar 시간 범위 | `12:10–13:30 UTC` |

Spark는 JSON schema parsing, 필수 필드·종목·가격·수량 검증, UTC event-time 변환, `event_id` 중복 제거, 2분 watermark와 1분 OHLCV·거래 건수·VWAP 집계를 수행했다.

`1,576 - 509 = 1,067`은 validation 오류나 유실 건수가 아니다. append mode에서 실행 종료 시점까지 watermark를 통과하지 않은 window의 추정치다. 정확한 가격 행, 원본 payload, API key와 checkpoint는 Git에 포함하지 않는다.

## 프로젝트 내 역할

```text
BLS CPI 발표 시각 + ALFRED 당시 값
                    ↓ 같은 event time
Alpaca IEX raw trades → Kafka → Spark → IEX 1분봉
Alpaca SIP 1분봉      → batch analysis → 시장 반응·coverage 비교
```

IEX 경로는 무료 실시간 수집을 재현하는 예비 관측 경로이고 SIP는 더 넓은 시장 범위의 과거 분석 경로다. 두 feed를 서로 덮어쓰거나 같은 baseline으로 섞지 않는다.

기계 판독용 공개 집계는 [result.json](../evidence/cpi-kafka-spark/result.json)에 있다.
