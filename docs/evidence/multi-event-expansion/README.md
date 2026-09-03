# 다중 경제 이벤트 확장 실행 증거

## 전체 실행 결과

2026-09-03에 공식 발표 202회와 10종목을 대상으로 분석용 시장 데이터 수집을 완료했습니다.

| 항목 | 실제 결과 |
| --- | ---: |
| CPI / 고용 / PCE / FOMC | 55 / 55 / 55 / 37 |
| 공식 발표 합계 | 202 |
| 종목 | 10 |
| 발표-종목 구간 | 2,020 |
| Alpaca 요청 수 | 404(페이지 추가 전 기준) |
| 실제 응답 페이지 | 404 |
| SIP 1분봉 선택 합계 | 308,512 |
| 파생 3분봉 | 112,593 |
| └ PARTIAL | 19,178 |
| 파생 5분봉 | 70,090 |
| └ PARTIAL | 16,215 |
| SIP 일봉 선택 합계 | 30,250 |
| FRED·ALFRED context | 2,020 |

행 수는 이벤트별 선택·생성 합계입니다. 같은 시장 시각이나 거래일이 여러 이벤트 범위에 포함될 수 있으므로 PostgreSQL은 동일 `symbol + bar_start + timeframe + source + feed`를 한 번만 저장합니다.

최종 DB 조회에서 경제 이벤트는 CPI 55, 고용 55, PCE 55, FOMC 37행이며 context는 각각 550, 550, 550, 370행입니다. `market_bars` 테이블 전체에는 이전 과제 데이터도 함께 있어 Alpaca SIP 고유 행이 1m 323,126, 3m 112,593, 5m 70,090, 1d 11,680행입니다. 따라서 이벤트별 선택 합계와 테이블 전체 행 수를 같은 값으로 해석하지 않습니다. 전체 business key 중복은 0건입니다.

## 일봉 coverage

| 상태 | 건수 | 확인 내용 |
| --- | ---: | --- |
| COMPLETE | 1,980 | 이전 7 + 발표일 1 + 이후 7거래일 |
| MARKET_CLOSED | 30 | Good Friday 발표 3회 × 10종목 |
| FUTURE_SESSION_UNAVAILABLE | 10 | 2026-08-26 PCE 이후 거래일 5일만 존재 |

`MARKET_CLOSED` 날짜는 2023-04-07 Employment, 2024-03-29 PCE, 2026-04-03 Employment입니다. 누락이나 미래 값을 합성하지 않았습니다.

## Airflow 실제 smoke

`market_context_backfill_pipeline`을 FOMC 2026-07-29와 SPY·TLT로 실행했습니다.

| 항목 | 결과 |
| --- | ---: |
| mapped event task | 1 |
| 종목별 work item | 2 |
| provider 요청 | 2 |
| 1m / 3m / 5m / 1d | 362 / 122 / 74 / 30 |
| 성공 work item / 미해결 alert | 2 / 0 |

전체 202회 시장 데이터는 DAG와 같은 `collect_market_context_event` 함수를 사용하는 CLI로 전수 실행했습니다. Airflow의 전체 202개 mapped task를 실행했다고 주장하지 않습니다. 실제 Airflow run ID와 task 상태는 [6차시 Airflow 증거](../sixth-assignment/README.md)에 있습니다.

## Kafka v2 실제 검증

기준 Parquet 118,118건을 `raw.market-sip.load.v2`에 재생했습니다.

| 단계 | 결과 |
| --- | ---: |
| 원시 / Kafka 발행 / 수신 / Spark 입력 | 모두 118,118 |
| Spark 형식 오류 / 실제 중복 | 0 / 0 |
| Spark 1분봉 / PostgreSQL 저장 | 472 / 472 |
| DB business key 중복 | 0 |
| 실행 시간 / 처리량 | 18.666초 / 6,327.881건·초 |
| 최대 파티션 비중 | 33.9% |

파티션별 건수는 40,069 / 29,314 / 15,098 / 5,580 / 9,793 / 18,264입니다. 기존 v1 최대 비중 97.5%보다 낮아졌지만 완전히 균등하지는 않습니다.

## 별도 원시 수집 smoke

FOMC 2026-07-29 14:00 ET의 TLT SIP 개별 체결도 `[13:00, 15:01) ET` 범위로 수집했습니다.

```text
raw trades: 29,139
API pages: 3
Parquet SHA-256: 77059c190cf58c9a90a2e52fb6abd3c26e6bf1979d240b680c5a21f1824bc1ff
```

이 결과는 Alpaca Historical Trades → Parquet 원본 보관 단계의 smoke입니다. 원본 Parquet은 `data/archive/`에 있고 Git에서 제외됩니다.

## 공개 파일

- [전체 확장 기계 판독 요약](full-expansion-summary.json)
- [Kafka v2 실행 결과](../load-recovery/v2-partition-routing.json)
- [Airflow 확장 smoke](../sixth-assignment/airflow-expansion-smoke.json)
- [FOMC·TLT 원시 수집 요약](fomc-tlt-2026-07-29.json)
- [기존 FOMC·TLT bar smoke](fomc-tlt-context.json)
- [전체 설명](../../multi-event-expansion.md)

`full-context-2026-09-03.json`은 초기 77회 확장 실행 기록으로 보존합니다. 현재 정본은 202회 실행을 담은 `full-expansion-summary.json`입니다.
