# CPI Historical SIP Backfill Result

검증일: 2026-08-24

## 범위

- 경제 이벤트: BLS CPI 실제 발표 12회
- 종목: `SPY`, `QQQ`, `SMH`, `NVDA`
- 시장 데이터: Alpaca Historical SIP `1Min` bar
- 구간: 각 발표 시각 60분 전부터 60분 후까지, 양 끝 포함
- 저장: PostgreSQL `market_bars`, `source=alpaca`, `feed=sip`

2025년 10월 CPI는 BLS에서 발표하지 않았으므로 이벤트 목록에 포함하지 않았다.

## 실행 결과

| 항목 | 결과 |
| --- | ---: |
| CPI 이벤트 | 12 |
| API page | 12 |
| 조회·upsert bar | 5,320 |
| 재실행 후 DB bar | 5,320 |
| 중복 business key | 0 |

| 종목 | bar 수 | 이론상 최대 1,452분 대비 |
| --- | ---: | ---: |
| NVDA | 1,444 | 99.4% |
| QQQ | 1,441 | 99.2% |
| SPY | 1,430 | 98.5% |
| SMH | 1,005 | 69.2% |

이론상 최대치는 `12회 × 121분`이다. Provider minute bar는 해당 분에 거래가 없으면 행이 없을 수 있으므로, 빠진 분을 곧바로 API 장애로 판정하거나 가격을 임의 생성하지 않는다. 특히 SMH 장전 구간은 다른 세 종목보다 희소하다. 분석 단계에서는 window 시작·종료 주변 bar 존재 여부와 coverage ratio를 함께 저장하고, 기준 미달 결과는 `PARTIAL_MARKET_COVERAGE`로 표시한다.

## 재현

```bash
.venv/bin/python -m src.cpi_market_backfill

docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/cpi_sip_coverage.sql
```

실제 가격 행과 API credential은 Git에 포함하지 않는다. 이 문서는 건수, 시간 범위와 데이터 품질 판단만 기록한다.
