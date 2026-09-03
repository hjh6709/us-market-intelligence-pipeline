# 다중 경제 이벤트·다종목 확장

## 한눈에 보는 범위

기존 실측 결과는 `CPI 55회 × SPY·QQQ·SMH·NVDA 4종목 = 220개 구간`입니다. 이번 변경은 이 결과를 지우거나 신규 결과처럼 포장하지 않고, 다음 수집 대상을 공식 발표 일정과 종목 역할로 명확히 정의합니다.

| event type | 공식 기관 | manifest 범위 | 이벤트 수 | 시장 데이터 상태 |
| --- | --- | --- | ---: | --- |
| CPI | BLS | 2022-01-12~2026-08-12 | 55 | 4종목 수집·Kafka·Spark·DB 검증 완료 |
| EMPLOYMENT | BLS | 2026-01-09~2026-08-07 | 8 | 공식 시각 검증, SIP 수집 예정 |
| PCE | BEA | 2026-01-22~2026-08-26 | 9 | 공식 시각 검증, SIP 수집 예정 |
| FOMC | Federal Reserve | 2026-01-28~2026-07-29 | 5 | 공식 시각 검증, SIP 수집 예정 |

따라서 현재 catalog는 77개 발표입니다. 종목 universe 10개를 모두 적용하면 770개 발표-종목 파티션입니다. `770`은 전체 다운로드 완료 수가 아니라 코드가 검증한 수집 계획 수입니다. 신규 수집기 smoke test로 `2026-07-29 FOMC × TLT` 1개 파티션은 실제 수집했습니다.

## 신규 실제 수집 결과

| 이벤트 | 종목 | 공식 시각 | 수집 구간 | SIP 원시 체결 | API 페이지 | 결과 |
| --- | --- | --- | --- | ---: | ---: | --- |
| FOMC | TLT | 2026-07-29 14:00 ET | 13:00~15:01 ET | 29,139 | 3 | Parquet·row count·SHA-256 manifest 완료 |

이 결과는 Alpaca Historical Trades → Parquet 원본 보관 단계까지의 검증입니다. Kafka 발행·Spark 집계·PostgreSQL 저장 결과는 아직 없으므로 완료로 합치지 않습니다. 공개 증거는 [multi-event-expansion evidence](evidence/multi-event-expansion/README.md)에 있습니다.

## 왜 종목을 10개로 늘렸나

| 종목 | 역할 | 해석 목적 |
| --- | --- | --- |
| SPY | 미국 대형주 시장 | 시장 전체 기준 |
| QQQ | 성장주 | 금리 변화에 민감한 성장주 비교 |
| IWM | 소형주 | 경기·자금조달 비용 반응 |
| TLT | 장기 미국채 ETF | 금리 기대가 채권 가격에 반영되는지 확인 |
| XLF | 금융 ETF | 금리·경기 변화에 민감한 금융업 비교 |
| SMH | 반도체 ETF | 고변동 성장 업종 비교 |
| GLD | 금 ETF | 인플레이션·위험회피 반응 비교 |
| NVDA | 반도체 개별주 | 기존 원시 체결 검증과 연속성 유지 |
| AAPL | 대형 기술주 | 개별 대형 성장주 비교 |
| JPM | 대형 은행주 | 금융 ETF와 개별 은행주 차이 비교 |

ETF를 포함한 이유는 개별 기업 뉴스의 영향을 줄이고 경제지표에 대한 시장·섹터 반응을 비교하기 위해서입니다. 개별주 NVDA·AAPL·JPM은 섹터 ETF와 실제 개별 기업 반응이 얼마나 다른지 확인하는 용도입니다.

## 시간 범위는 세 층으로 나눈다

한 가지 해상도로 모든 질문에 답하지 않습니다. Kafka·Spark가 원시 거래를 정확히 처리하는지, 발표 당일 반응이 어땠는지, 발표 전후 며칠 동안 흐름이 이어졌는지는 필요한 데이터 양과 해상도가 다르기 때문입니다.

| 계층 | 정확한 범위 | 입력 데이터 | 목적 | 현재 상태 |
| --- | --- | --- | --- | --- |
| `CORE_RAW_121M` | `T-60분`부터 `T+60분`에 시작하는 분까지 | SIP 개별 체결 | Kafka·Spark 전처리와 1분봉 정합성 검증 | CPI 55회 × 4종목 실행 완료 |
| `SESSION_1MIN` | `T-60분`부터 `T+120분`에 시작하는 분까지, 최대 181개 | Alpaca SIP 1분봉 | 발표 직후 5·30·60분과 당일 후속 반응 분석 | FOMC×TLT 181행 smoke 완료, 전체 실행 전 |
| `DERIVED_3M_5M` | 같은 181분의 1분봉을 3분·5분 단위로 집계 | 저장된 1분봉 | 희소 구간을 보기 쉬운 분석 해상도 제공 | FOMC×TLT 61행·37행 smoke 완료 |
| `DAILY_15_SESSIONS` | 발표 거래일을 포함해 이전 7거래일 + 이후 7거래일 | Alpaca SIP 일봉 | 발표 전 움직임과 1·3·7거래일 후 흐름 분석 | FOMC×TLT 15행 smoke 완료, 전체 실행 전 |

`전후 7일`은 달력 날짜가 아니라 실제 15개 거래 세션입니다. 주말과 휴장일은 행을 만들지 않습니다. 최신 발표가 아직 7거래일이 지나지 않았다면 부족한 이후 세션 수를 manifest에 `incomplete`로 남기며 미래 값을 채우지 않습니다.

14일치 원시 체결을 모두 Kafka에 넣지 않는 이유는 일별 방향을 보는 데 틱 단위 데이터가 불필요하기 때문입니다. 원시 체결은 핵심 121분 검증에만 사용하고, 넓은 구간은 provider가 집계한 1분봉·일봉을 받아 PostgreSQL의 서로 다른 `timeframe`으로 저장합니다.

3분봉과 5분봉은 별도 API 데이터가 아니라 저장 직전의 1분봉에서 파생합니다. OHLC는 시간 순서대로 집계하고 거래량·거래 건수는 합산하며 VWAP은 거래량 가중평균으로 계산합니다. 포함된 원본 분이 3/3 또는 5/5면 `COMPLETE`, 부족하면 `PARTIAL`입니다. 181분의 마지막 한 분은 정확히 나누어떨어지지 않으므로 smoke 결과의 마지막 3분봉·5분봉 각 1개가 `PARTIAL`인 것이 정상입니다.

- BLS CPI·고용, BEA PCE: 보통 08:30 ET 발표이므로 07:30~10:30 ET의 장전과 장 시작 후 반응을 함께 봅니다.
- FOMC statement: 14:00 ET 발표이므로 13:00~16:00 ET의 정규장 반응을 봅니다.
- 거래가 전혀 없거나 가격 형성 조건을 충족하는 거래가 없는 분은 임의로 채우지 않고 coverage 결측으로 남깁니다.

## 코드 흐름

```text
공식 발표 manifest(config/*_releases.json)
                +
종목 universe(config/market_universe.json)
                ↓
collect_market_event_archive.py
                ↓  event × symbol × [T-60m,T+61m)
Alpaca Historical Trades API (SIP)
                ↓
Parquet 원본 + partition manifest(row count, checksum)
                ↓
Kafka replay → Spark 검증·집계 → PostgreSQL
```

현재 새 스크립트는 공식 일정·종목·시간 구간을 일반화하고 Parquet 수집까지 담당합니다. 기존 Kafka replay와 Spark 처리기는 `event_type`과 `symbol`을 이미 메시지/파티션 필드로 사용하므로 같은 원본을 후속 처리할 수 있습니다. 다만 770개 파티션 전체의 신규 Kafka·Spark 실행 결과는 아직 생성하지 않았습니다.

분석용 두 구간은 `collect_market_event_context.py`가 같은 공식 일정과 종목 목록을 사용해 수집합니다. 77회 × 10종목 기준 계획상 최대치는 `SESSION_1MIN` 139,370행, 파생 3분봉 46,970행, 파생 5분봉 28,490행과 `DAILY_15_SESSIONS` 11,550행입니다. 이는 실제 저장 완료 건수가 아니라 결측이 하나도 없다고 가정한 상한입니다. 실제 smoke에서는 FOMC×TLT의 181개 1분봉, 61개 3분봉, 37개 5분봉과 15개 일봉을 PostgreSQL에서 확인했습니다.

## 확인 및 단계 실행

```bash
# API 호출 없이 전체 계획 확인
.venv/bin/python scripts/collect_market_event_archive.py --dry-run

# 분석용 181분·15거래일 계획 확인
.venv/bin/python scripts/collect_market_event_context.py --dry-run

# 먼저 FOMC와 금리 민감 4종목만 수집
.venv/bin/python scripts/collect_market_event_archive.py \
  --event-types FOMC \
  --symbols SPY QQQ TLT XLF \
  --release-from 2026-01-01 --release-to 2026-08-31

# 이후 전체 catalog·universe 수집
.venv/bin/python scripts/collect_market_event_archive.py

# 1분봉·일봉을 수집해 PostgreSQL에 timeframe 1m·1d로 Upsert
.venv/bin/python scripts/collect_market_event_context.py
```

대량 수집은 `FOMC → 고용 → PCE → 기존 CPI의 신규 6종목` 순으로 나눕니다. 중간 실패 시 완료 manifest와 Parquet checksum이 일치하는 파티션은 건너뛰고 실패한 파티션부터 이어서 실행합니다.

## 완료 판정

신규 범위는 아래 항목을 모두 만족할 때만 `수집 완료`로 바꿉니다.

1. 계획 파티션 수와 생성된 유효 manifest 수가 일치한다.
2. 각 manifest의 row count가 실제 Parquet row count와 일치한다.
3. 파일 SHA-256 checksum이 manifest와 일치한다.
4. Kafka 발행·Consumer 수신·Spark 입력 건수가 일치한다.
5. PostgreSQL 고유키 중복이 0건이며 누락 분은 coverage 사유로 구분된다.
