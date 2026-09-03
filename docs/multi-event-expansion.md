# 다중 경제 이벤트·다종목 확장

## 한눈에 보는 결과

기존 원시 체결 부하 실험은 `CPI 55회 × 4종목`입니다. 이번에는 분석용 provider bar 경로를 **공식 발표 202회 × 10종목 = 2,020개 구간**으로 확장해 실제 수집·저장했습니다. 원시 체결과 이미 집계된 bar는 행의 의미가 다르므로 건수를 섞지 않습니다.

| event type | 공식 기관 | 기간 | 발표 수 |
| --- | --- | --- | ---: |
| CPI | BLS | 2022-01-12~2026-08-12 | 55 |
| EMPLOYMENT | BLS | 2022-01-07~2026-08-07 | 55 |
| PCE | BEA | 2022-01-28~2026-08-26 | 55 |
| FOMC | Federal Reserve | 2022-01-26~2026-07-29 | 37 |
| **합계** |  |  | **202** |

| 분석 데이터 | 실제 합계 |
| --- | ---: |
| 발표 T-60~T+120분 SIP 1분봉 | 308,512 |
| 파생 3분봉 | 112,593 |
| 파생 5분봉 | 70,090 |
| 발표 전후 7거래일 SIP 일봉 | 30,250 |

위 값은 이벤트별 선택·생성 건수를 더한 것입니다. 여러 발표가 같은 시장 시각이나 거래일을 공유하면 PostgreSQL은 동일 business key로 한 번만 저장합니다.

## 왜 이 10종목인가

| 종목 | 역할 |
| --- | --- |
| SPY | 미국 대형주 시장 기준 |
| QQQ | 성장주·금리 민감도 |
| IWM | 소형주·자금조달 민감도 |
| TLT | 장기 국채 가격 |
| XLF | 금융 섹터 |
| SMH | 반도체 섹터 |
| GLD | 물가·위험회피 비교 |
| NVDA | 기존 원시 체결 실험과 연속성 |
| AAPL | 대형 기술 개별주 |
| JPM | 대형 은행 개별주 |

ETF를 함께 사용하면 개별 기업 뉴스만으로 움직인 결과와 시장·섹터의 공통 반응을 구분하는 데 도움이 됩니다. 다만 이 종목 선택만으로 경제지표의 인과 효과가 증명되지는 않습니다.

## 세 가지 시간 해상도

| 계층 | 범위 | 입력 | 목적 |
| --- | --- | --- | --- |
| `CORE_RAW_121M` | T-60~T+60분 | SIP 개별 체결 | Kafka·Spark 정확성·부하 검증 |
| `SESSION_1MIN` | T-60~T+120분, 최대 181개 분 | Alpaca SIP 1분봉 | 발표 직후와 당일 반응 분석 |
| `DAILY_15_SESSIONS` | 이전 7 + 발표일 + 이후 7거래일 | Alpaca SIP 일봉 | 발표 전후 며칠의 흐름 분석 |

3분봉과 5분봉은 `SESSION_1MIN`을 묶어 생성합니다. OHLC는 시간 순서, 거래량·거래 건수는 합계, VWAP은 거래량 가중평균으로 계산합니다. 원본 분이 3/3 또는 5/5면 `COMPLETE`, 부족하면 `PARTIAL`입니다.

- 8시 30분 발표는 07:30~10:30 ET로 장전을 포함합니다.
- FOMC 14시 발표는 13:00~16:00 ET 정규장을 봅니다.
- 거래가 없던 분은 가격을 채우지 않습니다.
- 3분·5분봉은 결측을 숨기기 위한 대체 값이 아닙니다.

## 수집과 자동화 구조

```text
공식 발표 manifest + 10종목
        ↓
Airflow: 경제발표 한 건 = mapped task 한 개
        ↓
Alpaca 다종목 요청
  ├─ 10종목 1분봉 한 번
  └─ 10종목 일봉 한 번
        ↓
1m·1d 저장 + 3m·5m 생성
        ↓
PostgreSQL market_bars
```

이전의 종목별 요청 설계라면 `202 × 10 × 2 = 4,040`회가 필요합니다. 현재 구현은 종목을 묶어 `202 × 2 = 404`회(페이지 추가 전 기준)로 줄였습니다. DB에는 2,020개 종목별 work item과 quality check를 기록할 수 있어, 묶어서 요청하더라도 실패한 종목과 coverage를 구분합니다.

다년 범위는 `market_context_backfill_orchestrator`가 연도별 child DAG run으로 나눕니다. child DAG는 발표별 task를 만들므로 실패한 연도·발표 범위만 다시 실행할 수 있습니다.

## coverage 결과

### 장중 bar

| timeframe | 전체 | COMPLETE | PARTIAL |
| --- | ---: | ---: | ---: |
| 3m | 112,593 | 93,415 | 19,178 |
| 5m | 70,090 | 53,875 | 16,215 |

`PARTIAL`은 저장 실패가 아니라 해당 묶음 안에 실제 1분봉이 기대 개수보다 적다는 뜻입니다.

### 일봉

| 상태 | 발표-종목 수 | 설명 |
| --- | ---: | --- |
| COMPLETE | 1,980 | 7 / 1 / 7 거래일 확보 |
| MARKET_CLOSED | 30 | Good Friday 발표 3회 × 10종목 |
| FUTURE_SESSION_UNAVAILABLE | 10 | 2026-08-26 PCE 이후 거래일 5일만 확보 |

휴장 발표일은 2023-04-07 Employment, 2024-03-29 PCE, 2026-04-03 Employment입니다. 최신 PCE의 미래 두 거래일도 임의로 만들지 않았습니다.

## 경제 맥락 결합

같은 공식 발표 202회마다 FRED·ALFRED 10개 series의 당시 이용 가능 값을 연결합니다.

`CPIAUCSL, CPILFESL, PCEPI, PCEPILFE, UNRATE, PAYEMS, DFF, DGS2, DGS10, VIXCLS`

이 10개 값은 발표 당시의 물가·고용·금리·변동성 환경입니다. 각 발표의 시장 전망치나 surprise를 뜻하지 않습니다. 실제값·전망치·surprise는 별도 point-in-time 출처를 검증한 뒤 추가해야 합니다.

## 실행

```bash
# 계획만 확인
.venv/bin/python scripts/collect_market_event_context.py --dry-run

# 시장 데이터 전체 실행
.venv/bin/python scripts/collect_market_event_context.py \
  --event-types CPI EMPLOYMENT PCE FOMC \
  --release-from 2022-01-01 --release-to 2026-08-26 \
  --symbols SPY QQQ IWM TLT XLF SMH GLD NVDA AAPL JPM

# 경제 맥락 전체 실행
.venv/bin/python scripts/collect_macro_event_context.py \
  --event-types CPI EMPLOYMENT PCE FOMC \
  --release-from 2022-01-01 --release-to 2026-08-26
```

대용량 원본과 전체 manifest는 `data/archive/`에 두고 Git에서 제외합니다. 공개 저장소에는 가격과 비밀정보가 없는 집계 결과만 [evidence](evidence/multi-event-expansion/README.md)에 남깁니다.

## 이벤트 반응 분석과 탐색용 백테스트

저장한 1분봉을 공식 `released_at` 기준으로 `PRE_60M`, `POST_5M`, `POST_30M`, `POST_60M` 네 구간으로 계산했습니다. 202회 × 10종목 × 4구간으로 `macro_event_impacts`에 **8,080행**을 저장했습니다. 각 행에는 수익률, 거래량, 분 단위 수익률의 변동성, SPY 대비 수익률과 coverage가 있습니다.

전망치와 surprise가 아직 없으므로 경제지표 방향을 예측하는 전략은 만들지 않았습니다. 대신 시점 누수를 피할 수 있는 가장 단순한 탐색 기준을 실행했습니다.

```text
발표 전 60분 수익률 > 0 → 발표 시점 long
발표 전 60분 수익률 < 0 → 발표 시점 short
발표 60분 후 청산
왕복 거래비용 10bp 차감
```

| 항목 | 실제 결과 |
| --- | ---: |
| 전체 발표-종목 결과 | 2,020 |
| 가격이 있어 실행 가능한 결과 | 1,988 |
| 전·후 coverage가 모두 COMPLETE | 911 |
| 평균 비용 차감 수익률 | -0.1565% |
| 중앙값 비용 차감 수익률 | -0.1251% |
| 비용 차감 후 양수 비율 | 39.34% |

이 결과는 현재 규칙에 수익성이 없다는 뜻입니다. 여러 종목을 동시에 운용한 포트폴리오 수익률도 아니고, 경제지표의 인과 효과나 미래 예상 수익률도 아닙니다. `PARTIAL` 장전 데이터까지 포함한 전체 결과와 `COMPLETE` 표본 수를 따로 기록했으며, 공개 집계는 [event-analysis.json](evidence/multi-event-expansion/event-analysis.json)에 있습니다.
