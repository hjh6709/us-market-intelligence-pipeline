# CPI Event Impact Initial Result

검증일: 2026-08-24

## 계산 정의

- 대상: CPI 발표 12회, `SPY`, `QQQ`, `SMH`, `NVDA`
- `PRE_60M`: 발표 60분 전 첫 bar open부터 발표 직전 마지막 bar close까지
- `POST_5M/30M/60M`: 발표 직전 마지막 close부터 각 발표 후 구간의 마지막 close까지
- 거래량: 해당 window에 실제 존재하는 SIP bar volume 합계
- 변동성: baseline과 각 bar close 사이의 1분 수익률에 대한 population standard deviation
- benchmark: 동일 이벤트·window의 SPY 수익률
- 상대수익률: 종목 수익률 - SPY 수익률
- 분석 버전: `cpi_sip_v1`

결측 분을 forward-fill하지 않는다. 기대 분봉의 90% 미만이거나 구간 끝 가격이 오래되면 `PARTIAL_MARKET_COVERAGE`로 저장한다.

## 적재·품질 결과

| 항목 | 결과 |
| --- | ---: |
| 경제 이벤트 | 12 |
| 종목 | 4 |
| window | 4 |
| `macro_event_impacts` | 192 |
| benchmark 누락 | 0 |
| SPY 상대수익률 오류 | 0 |
| 중복 business key | 0 |
| 재실행 후 row | 192 |

## 12회 단순 평균 수익률

단위는 percent다. 아래 값은 CPI의 인과 효과나 미래 수익 예측이 아니라, 현재 선택한 12개 발표 구간에서 관측된 단순 평균이다.

| 종목 | 발표 전 60분 | 발표 후 5분 | 발표 후 30분 | 발표 후 60분 |
| --- | ---: | ---: | ---: | ---: |
| SPY | 0.0705 | 0.1340 | 0.1422 | 0.1837 |
| QQQ | 0.0935 | 0.2154 | 0.2054 | 0.2673 |
| SMH | 0.1164 | 0.4862 | 0.4822 | 0.5492 |
| NVDA | 0.1037 | 0.2668 | 0.3370 | 0.4557 |

SMH의 complete event 수는 `PRE_60M 3/12`, `POST_5M 8/12`, `POST_30M 5/12`, `POST_60M 5/12`다. 따라서 SMH 평균은 다른 종목보다 coverage 제약이 크며, 다음 비교 분석에서는 complete 결과와 partial 결과를 분리해야 한다.

현재 단계에는 발표 전망치, actual-forecast surprise, 같은 요일·시간대의 비발표 baseline과 통계적 유의성 검정이 없다. 이 항목을 추가하기 전에는 “경제지표가 이런 결과를 만들었다”고 결론 내리지 않는다.

## 재현

```bash
.venv/bin/python -m src.macro_event_impact

docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/cpi_event_impact_summary.sql
```
