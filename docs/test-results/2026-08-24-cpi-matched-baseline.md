# CPI Matched Baseline Initial Result

검증일: 2026-08-24

## 비교군 정의

각 CPI 발표일의 1·2·3주 전 같은 요일, 같은 미국 동부 현지시각을 비교군으로 사용한다. 서머타임 전환이 있더라도 `08:30 ET`끼리 비교하고 UTC 시각만 달라지도록 계산한다.

```text
CPI 발표일 08:30 ET
↔ 1주 전 같은 요일 08:30 ET
↔ 2주 전 같은 요일 08:30 ET
↔ 3주 전 같은 요일 08:30 ET
```

각 비교일에도 발표 전 60분과 이후 5·30·60분을 계산한다. bar를 임의 생성하거나 forward-fill하지 않으며 `COMPLETE`인 비교군만 이벤트별 평균에 사용한다.

## 실제 적재 결과

| 항목 | 결과 |
| --- | ---: |
| CPI 이벤트 | 12 |
| 비교 시간창 | 36 |
| baseline metric | 576 |
| complete | 431 |
| partial coverage | 111 |
| no market data | 34 |
| 전체 SIP bar | 19,933 |
| 중복 baseline key | 0 |
| 재실행 후 baseline metric | 576 |

2025년 11월 27일 추수감사절과 2026년 4월 3일 Good Friday는 시장 데이터가 없는 휴장 비교일로 확인됐다. 이 결과는 감사 추적을 위해 `NO_MARKET_DATA`로 남기되 평균에서는 제외한다. 일부 SMH window도 장전 거래 희소성 때문에 no-data 또는 partial이다.

`macro_event_impacts` 192건 중 162건에 complete baseline 평균이 연결됐다.

| complete baseline 표본 수 | event impact 수 |
| ---: | ---: |
| 3 | 118 |
| 2 | 33 |
| 1 | 11 |
| 0 | 30 |

## 품질 필터 적용 비교

아래는 이벤트 coverage가 `COMPLETE`이고 complete baseline이 2개 이상인 결과만 집계한 것이다. `return difference`는 이벤트 수익률에서 matched baseline 평균 수익률을 뺀 percentage point이며, volume ratio는 이벤트 거래량을 baseline 평균 거래량으로 나눈 값이다.

| 종목 | window | 사용 이벤트 | 평균 return difference | 평균 volume ratio |
| --- | --- | ---: | ---: | ---: |
| SPY | POST_5M | 12 | 0.1503 | 13.507 |
| SPY | POST_30M | 12 | 0.1860 | 4.495 |
| SPY | POST_60M | 12 | 0.2028 | 2.681 |
| QQQ | POST_5M | 12 | 0.2370 | 8.484 |
| QQQ | POST_30M | 12 | 0.2693 | 3.409 |
| QQQ | POST_60M | 12 | 0.3020 | 2.256 |
| NVDA | POST_5M | 12 | 0.2840 | 4.777 |
| NVDA | POST_30M | 12 | 0.3937 | 2.343 |
| NVDA | POST_60M | 12 | 0.5498 | 1.489 |
| SMH | POST_5M | 2 | 0.7828 | 7.177 |
| SMH | POST_30M | 3 | 1.0532 | 1.867 |
| SMH | POST_60M | 2 | 1.7037 | 1.093 |

## 해석 제한

- 같은 요일·시간만 맞춘 초기 비교군이며 다른 경제 발표, 기업 뉴스와 시장 국면을 통제하지 않았다.
- 결과가 양수라는 사실만으로 CPI가 상승 원인이라고 결론 내릴 수 없다.
- SMH는 사용 가능한 이벤트가 2~3개뿐이므로 요약 수치를 일반화하지 않는다.
- 전망치와 actual surprise가 없으므로 물가 결과의 방향·크기와 시장 반응의 관계를 아직 분석하지 않는다.
- 다음 단계에서 미국 거래일 calendar와 다른 주요 경제 발표 calendar를 이용해 비교군을 더 엄격히 정제해야 한다.

## 재현

```bash
.venv/bin/python -m src.cpi_matched_baseline

docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/cpi_matched_baseline_summary.sql
```
