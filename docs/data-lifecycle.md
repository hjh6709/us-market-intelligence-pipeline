# MVP Data Collection and Lifecycle

상태: initial operating policy

기준일: 2026-08-13

이 문서는 4주 MVP에서 **무엇을 얼마나 수집하고, 어디에 저장하며, 무엇에 사용하고, 언제 삭제하는지**를 한곳에 정의한다. 실제 event 수와 byte 크기는 live smoke test 전에는 알 수 없으므로 임의로 만들지 않고, 수집 범위와 보존 한도를 먼저 고정한 뒤 측정값을 기록한다.

## 1. 수집 범위와 기간

### 분석 종목

- 약 22개 미국 주식·ETF
- 시장 기준: `SPY`, `QQQ`
- 반도체 기준: `SMH`, `SOXX`
- 주요 반도체·Nasdaq 종목: 2회차에 최종 allowlist 확정
- `SOXL`, `SOXS`: 관찰 또는 후속 simulation용이며 시장 기준선에서는 제외

### 세션 범위

P0 이상 징후 탐지는 미국 정규장 `09:30–16:00 America/New_York`만 사용한다. 장전·장후 event는 수신 상태를 smoke test로 관찰할 수 있지만 정규장 baseline과 섞지 않으며 P0 alert 계산에서는 제외한다. 휴장일과 조기 종료일은 Alpaca Calendar 결과를 따른다.

### 프로젝트 수집 기간

- live 수집 시작: 3회차 collector integration이 통과한 다음 거래일부터
- live 수집 종료 목표: 최종 발표일 2026-09-12
- 최소 성공 목표: 정규장 10거래일 이상의 live 또는 recorded run
- baseline warm-up: 발표 기간만 기다리지 않고, 시작 시점에 완료된 과거 정규장 20거래일의 IEX/SIP 1분 bar를 feed별로 backfill

20거래일 warm-up은 초기 운영값이다. 결측률과 feature 안정성을 측정한 뒤 변경하면 version과 근거를 남긴다.

## 2. 데이터별 수집·저장·활용·삭제 정책

| 데이터 | 수집량·주기 | 저장 위치 | 활용 | 보존·삭제 |
| --- | --- | --- | --- | --- |
| Alpaca IEX raw trade | 22종목 정규장 동안 수신되는 유효 trade 전체 | Streaming Node Kafka `raw.market.v1` | Spark 1분 OHLCV 집계, 지연·중복·처리량 측정 | Kafka time retention 24시간 후 자동 삭제. PostgreSQL에 raw tick 장기 저장 안 함 |
| IEX 1분 bar | 최대 `22 × 390 = 8,580 rows/정규 거래일` | Data/Batch Node PostgreSQL `market_bars`, `feed=iex` | 실시간 feature와 `PRELIMINARY_IEX` alert | 90일 rolling retention. 프로젝트 중에는 90일 미만이므로 유지 |
| SIP 1분 bar | Airflow가 15분마다 `window_end <= now-20m`인 미수집 구간을 batch 조회. 최대 8,580 rows/거래일 | PostgreSQL `market_bars`, `feed=sip` | IEX/SIP bar 비교, SIP 전용 feature, alert 확정·기각 | 90일 rolling retention. IEX bar를 덮어쓰지 않음 |
| Technical feature | feed별 1분 snapshot. 최대 bar 수와 같은 차수 | PostgreSQL `technical_features` | `return_5m`, volume Z-score, ATR-normalized move와 alert 근거 | 90일 rolling retention |
| Alert/reconciliation | 조건을 만족한 alert와 해당 SIP 재평가만 생성 | PostgreSQL `anomaly_alerts`, reconciliation/history tables | 경고 근거, 예비/확정/기각 상태, 감사 이력 | 90일 보존. 발표 결과 snapshot은 보고서로 별도 보존 |
| FRED macro | 9개 series, Airflow daily `14:00 UTC`, 최근 7일 overlap 조회 | PostgreSQL `macro_observations` | alert 시각 이전에 알려진 금리·물가·고용·변동성 환경 조회 | 데이터량이 작아 MVP에서는 자동 삭제하지 않음. revision/vintage 보존 |
| Replay fixture | 정상 60분 구간 1개와 duplicate/late/invalid/spike 시나리오 | 작은 fixture는 Git, 큰 capture는 로컬/OCI volume | 장외 데모, 1x·10x·50x·100x 부하 테스트, 장애 회귀 테스트 | 작은 deterministic fixture는 계속 보존. 임시 live raw capture는 최종 발표 30일 후 삭제 |
| DLQ | validation 실패 또는 처리 불가 event | Kafka `dead-letter.v1`; 필요 시 오류 metadata만 PostgreSQL | 오류 유형·건수 확인과 재현 | 7일 후 자동 삭제 |
| Pipeline log/metric | 실행 중 structured log와 run별 집계 metric | 각 node log volume, 결과 요약은 report | lag, latency, CPU/RAM, recovery time 설명 | 원본 log 14일, 요약 report는 repository에 계속 보존 |
| News metadata — 선택 | 22종목 관련 기사만, 구현 시 수집 | PostgreSQL metadata/event tables | alert 주변의 관련 기사 후보 조회 | metadata 30일. 기사 전체 본문은 저장하지 않음 |

SIP DAG의 `now-20m`은 무료 historical SIP의 15분 제한에 5분 safety margin을 둔 초기값이다. 실제 account smoke test 후 schedule을 조정해도 `end <= now-15m` 계약은 위반하지 않는다.

## 3. Feature와 분석에서 실제로 사용하는 방법

### IEX 실시간 탐지

```text
IEX raw trade
→ trade condition 적용
→ 종목·event-time 1분 OHLCV/VWAP/count
→ IEX 과거 20거래일 baseline
→ return_5m + volume_zscore + atr_normalized_move
→ PRELIMINARY_IEX
```

거래량은 장중 시간대 효과가 크므로 초기 `volume_zscore`는 가능하면 과거 20거래일의 **같은 minute-of-session**과 비교한다. 데이터가 부족하면 alert를 생성하지 않고 `INSUFFICIENT_WARMUP`을 기록한다. 정확한 threshold는 fixture와 live 분포를 본 뒤 version 1로 고정한다.

### SIP 지연 검증

```text
같은 symbol·같은 정규장 window의 SIP bar
→ SIP 과거 20거래일 baseline
→ 동일 feature contract 재계산
→ IEX와 SIP 관측값/coverage 비교
→ CONFIRMED_SIP 또는 REJECTED_AFTER_RECONCILIATION
```

IEX feature를 SIP baseline에 비교하거나 SIP 값으로 IEX row를 수정하지 않는다. SIP 조회 실패나 누락은 기각이 아니라 `PRELIMINARY_IEX` 유지 사유다.

### FRED 환경 설명

| Series | 사용할 값 | 해석 범위 |
| --- | --- | --- |
| `DGS2`, `DGS10` | 최신 이용 가능 수준, 전 관측일 변화, `DGS10-DGS2` | 단기·장기 금리와 장단기 금리차 환경 |
| `DFF` | 최신 이용 가능 수준과 변화 | 정책금리 환경 |
| `CPIAUCSL`, `CPILFESL` | 최근 발표값과 직전 발표 대비 변화 | 소비자물가 환경 |
| `PCEPI`, `PCEPILFE` | 최근 발표값과 직전 발표 대비 변화 | PCE 물가 환경 |
| `UNRATE` | 최근 발표값과 직전 발표 대비 변화 | 고용 환경 |
| `VIXCLS` | 최신 이용 가능한 수준과 변화 | 시장 변동성 환경 |

Alert 시각보다 늦게 수집·발표된 값을 과거 설명에 사용하지 않는다. FRED 값은 “주가가 움직인 원인”으로 단정하지 않고 alert 당시 이용 가능했던 배경 정보로만 표시한다.

## 4. 최종 조회 결과

MVP에서 한 alert를 설명할 때 다음 묶음을 조회할 수 있어야 한다.

```text
alert id, symbol, event time, status
+ IEX 1분 bar와 return/volume/ATR feature
+ 사용한 IEX threshold와 baseline version
+ SIP 동일 구간 feature와 reconciliation 결과
+ SPY/QQQ/SMH/SOXX 동일 구간 변화
+ alert 시각 기준 최신 FRED 환경
+ 데이터 freshness와 pipeline 상태
```

이 결과는 인과관계나 투자 권유가 아니라 **무엇을 관측했고, 어느 범위의 데이터로 검증했는지**를 보여준다.

## 5. 저장 위치와 백업 경계

```text
Streaming Node (OCI A1 1 OCPU / 6GB, proposed)
├── Kafka data volume: raw trade 24h, DLQ 7d
├── Spark checkpoint volume
└── temporary capture volume

Data/Batch Node (OCI A1 1 OCPU / 6GB, proposed)
├── PostgreSQL volume: bars, features, macro, alerts, reconciliation
├── Airflow metadata/log volume
└── scheduled database dump

GitHub repository
├── schema/migration/code
├── small deterministic fixtures
└── aggregate test reports
```

API key, 전체 raw capture, PostgreSQL volume과 database dump는 Git에 올리지 않는다. PostgreSQL dump는 발표 전과 schema 변경 전 생성해 개발 PC 또는 별도 허용된 backup 위치에 보관한다. OCI 2노드 구조는 ARM64와 자원 smoke test가 통과하기 전까지 proposed 상태다.

## 6. 측정 후 바꿀 수 있는 항목

다음 값은 초기 정책이지만 조용히 변경하지 않는다.

- 정확한 22개 symbol allowlist
- live event/byte 수와 Kafka disk 사용량
- volume baseline window와 alert threshold
- watermark delay와 SIP safety margin
- PostgreSQL 90일 retention이 실제 disk에 미치는 영향
- OCI node별 container memory limit

변경 시 측정 결과, 이전 값, 새 값, 변경 이유를 load-test report 또는 ADR에 기록한다.
