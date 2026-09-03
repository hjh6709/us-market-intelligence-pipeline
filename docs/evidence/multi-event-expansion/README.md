# 다중 경제 이벤트 확장 실행 증거

## 확인된 실제 실행

```text
event_type: FOMC
official release: 2026-07-29 14:00 ET
symbol: TLT
window: [13:00, 15:01) ET
feed: SIP
raw trades: 29,139
API pages: 3
Parquet SHA-256: 77059c190cf58c9a90a2e52fb6abd3c26e6bf1979d240b680c5a21f1824bc1ff
```

수집기는 실제 Alpaca Historical Trades API 응답을 `data/archive/event_type=FOMC/release_date=2026-07-29/symbol=TLT/trades.parquet`에 저장했습니다. 원시 Parquet은 대용량·재배포 제한 때문에 Git에서 제외하고, 공개 저장소에는 비밀정보가 없는 실행 요약만 남깁니다.

이 증거가 확인하는 단계는 `공식 FOMC 시각 → TLT SIP 수집 → Parquet row count·checksum manifest`입니다. Kafka·Spark·PostgreSQL 처리는 아직 실행하지 않았으므로 이 문서에서 완료로 주장하지 않습니다.

## 분석 구간 확장 smoke

같은 `FOMC × TLT`를 대상으로 분석용 bar 수집과 PostgreSQL 저장도 별도로 확인했습니다.

| 계층 | 범위 | API 선택 | PostgreSQL 확인 | coverage |
| --- | --- | ---: | ---: | --- |
| `SESSION_1MIN` | 13:00~16:00 ET, 181개 분 | 181 | 181 | 전체 분 존재 |
| `DAILY_15_SESSIONS` | 이전 7 + 발표일 + 이후 7거래일 | 15 | 15 | 7 / 1 / 7 완료 |

181개의 1분봉에서는 분석 편의를 위해 다음 파생봉도 저장했습니다.

| 파생봉 | PostgreSQL 행 | COMPLETE | PARTIAL | 해석 |
| --- | ---: | ---: | ---: | --- |
| 3분봉 | 61 | 60 | 1 | 마지막 16:00 ET 봉에 원본 1분만 포함 |
| 5분봉 | 37 | 36 | 1 | 마지막 16:00 ET 봉에 원본 1분만 포함 |

부분 봉을 버리거나 가격을 채우지 않았습니다. 각 행의 `source_bar_count`, `expected_bar_count`, `coverage_status`로 실제 포함된 1분봉 수를 확인할 수 있습니다.

위 표는 구현 직후 확인한 한 이벤트·한 종목 smoke 결과입니다. 이후 2026-09-03에 77회 × 10종목 전체 분석용 bar 수집을 완료했습니다.

## 전체 분석 구간 실행 결과

| 항목 | 실제 결과 | 확인 의미 |
| --- | ---: | --- |
| 공식 발표 | 77회 | CPI 55, 고용 8, PCE 9, FOMC 5 |
| 종목 | 10개 | 총 770개 발표-종목 구간 |
| SIP 1분봉 | 117,566행 | 발표 T-60분~T+120분 범위의 실제 존재 봉 |
| 파생 3분봉 | 43,184행 | COMPLETE 35,255, PARTIAL 7,929 |
| 파생 5분봉 | 26,883행 | COMPLETE 20,205, PARTIAL 6,678 |
| 일봉 선택 | 11,520행 | 각 이벤트 관점의 전후 거래일 합계 |
| 일봉 DB 고유 행 | 8,740행 | 이벤트 사이에 겹치는 날짜를 고유키 Upsert한 결과 |

manifest의 1분·3분·5분 건수와 발표별 시간 범위로 재조회한 PostgreSQL 건수가 일치했습니다. 3분봉·5분봉의 `PARTIAL`은 없는 1분 가격을 채우지 않았다는 뜻이며, 저장 실패가 아닙니다.

일봉 coverage 770건 중 750건은 완전했고 20건은 사유가 확인된 미완전 구간입니다. 2026-04-03 고용보고서는 Good Friday 휴장이라 10종목 모두 발표일 일봉이 없었습니다. 2026-08-26 PCE는 실행 시점에 이후 거래일이 5일만 지나 10종목 모두 미래 2거래일이 아직 없었습니다.

- 기계 판독 요약: [`fomc-tlt-2026-07-29.json`](fomc-tlt-2026-07-29.json)
- 분석 구간 저장 요약: [`fomc-tlt-context.json`](fomc-tlt-context.json)
- 전체 실행 요약: [`full-context-2026-09-03.json`](full-context-2026-09-03.json)
- 전체 확장 설명: [`../../multi-event-expansion.md`](../../multi-event-expansion.md)
