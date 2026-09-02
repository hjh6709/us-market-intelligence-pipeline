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

이는 한 이벤트·한 종목의 smoke 결과입니다. 77회 × 10종목 전체가 저장됐다는 뜻은 아닙니다.

- 기계 판독 요약: [`fomc-tlt-2026-07-29.json`](fomc-tlt-2026-07-29.json)
- 분석 구간 저장 요약: [`fomc-tlt-context.json`](fomc-tlt-context.json)
- 전체 확장 설명: [`../../multi-event-expansion.md`](../../multi-event-expansion.md)
