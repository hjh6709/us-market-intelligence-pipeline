# MVP Data Collection and Lifecycle

상태: initial operating policy

기준일: 2026-08-13

최근 수정: 2026-08-24 멘토 피드백 반영

이 문서는 경제지표 영향 검증과 자동매매 데이터 기반을 만드는 4주 MVP에서 **무엇을 얼마나 수집하고, 어디에 저장하며, 무엇에 사용하고, 언제 삭제하는지**를 한곳에 정의한다. Stage A 데이터는 후속 전략·백테스트 입력으로 사용할 수 있어야 하지만 실제 주문에는 연결하지 않는다.

## 1. 수집 범위와 기간

### 분석 종목

- 약 22개 미국 주식·ETF
- 시장 기준: `SPY`, `QQQ`
- 반도체 기준: `SMH`, `SOXX`
- 주요 반도체·Nasdaq 종목: 2회차에 최종 allowlist 확정
- `SOXL`, `SOXS`: 관찰 또는 후속 simulation용이며 시장 기준선에서는 제외

### 세션 범위

실시간 이상 징후 탐지는 미국 정규장 `09:30–16:00 America/New_York`만 사용한다. 경제지표 영향 분석은 공식 발표 시각을 기준으로 별도 window를 사용한다. CPI·고용처럼 장전 발표는 extended-hours SIP coverage가 충분한 경우에만 즉시 반응을 계산하고, 그렇지 않으면 첫 정규장 반응을 별도 결과로 저장한다. 장전과 정규장 baseline을 섞지 않는다.

### 프로젝트 수집 기간

- live 수집 시작: 3회차 collector integration이 통과한 다음 거래일부터
- live 수집 종료 목표: 최종 발표일 2026-09-12
- 최소 성공 목표: 정규장 10거래일 이상의 live 또는 recorded run
- baseline warm-up: 발표 기간만 기다리지 않고, 시작 시점에 완료된 과거 정규장 20거래일의 IEX/SIP 1분 bar를 feed별로 backfill
- macro event-study 후보: 최근 24개월 CPI·Employment Situation·FOMC. 공식 일정과 1분 bar coverage smoke test 후 실제 분석 범위를 확정

20거래일 warm-up은 초기 운영값이다. 결측률과 feature 안정성을 측정한 뒤 변경하면 version과 근거를 남긴다.

## 2. 데이터별 수집·저장·활용·삭제 정책

| 데이터 | 수집량·주기 | 저장 위치 | 활용 | 보존·삭제 |
| --- | --- | --- | --- | --- |
| 공식 경제지표 발표 일정 | CPI·고용·FOMC 최근 24개월 후보, 이후 증분 확인 | PostgreSQL `economic_events` | 정확한 event time, reference period와 공식 source URL | MVP 자동 삭제 없음 |
| FRED/ALFRED observation·vintage | 9개 series, 일 1회 + event-study backfill | PostgreSQL `macro_observations` | 당시 공개된 actual/previous 값과 revision 추적 | MVP 자동 삭제 없음 |
| SIP macro event window | 각 event의 설정된 발표 전후 window, 약 22종목 | PostgreSQL `market_bars`, `feed=sip` | 수익률·거래량·변동성과 시장·섹터 비교 | 관련 impact와 함께 MVP 자동 삭제 없음; 범용 90일 cleanup에서 제외 |
| Macro impact 결과 | event×symbol×window×analysis version | PostgreSQL `macro_event_impacts`, reports | 반복된 반응, 표본 수, coverage와 한계 검증 | MVP 자동 삭제 없음 |
| Alpaca IEX raw trade | 22종목 정규장 동안 수신되는 raw trade 전체 | Streaming Node Kafka `raw.market.v1` | Spark parsing·검증·1분 OHLCV 집계, 지연·중복·처리량 측정 | Kafka time retention 24시간 후 자동 삭제. PostgreSQL에 raw tick 장기 저장 안 함 |
| IEX 1분 bar | 최대 `22 × 390 = 8,580 rows/정규 거래일` | Data/Batch Node PostgreSQL `market_bars`, `feed=iex` | 실시간 feature와 `PRELIMINARY_IEX` alert | 90일 rolling retention. 프로젝트 중에는 90일 미만이므로 유지 |
| SIP 1분 bar | Airflow가 15분마다 `window_end <= now-20m`인 미수집 구간을 batch 조회. 최대 8,580 rows/거래일 | PostgreSQL `market_bars`, `feed=sip` | IEX/SIP bar 비교, SIP 전용 feature, alert 확정·기각 | 90일 rolling retention. IEX bar를 덮어쓰지 않음 |
| Technical feature | feed별 1분 snapshot. 최대 bar 수와 같은 차수 | PostgreSQL `technical_features` | `return_5m`, volume Z-score, ATR-normalized move와 alert 근거; 후속 point-in-time 전략 연구 입력 | 90일 rolling retention |
| Alert/reconciliation | 조건을 만족한 alert와 해당 SIP 재평가만 생성 | PostgreSQL `anomaly_alerts`, reconciliation/history tables | 경고 근거, 예비/확정/기각 상태, 감사 이력; 후속 전략 평가 후보 | 90일 보존. 발표 결과 snapshot은 보고서로 별도 보존 |
| Replay fixture | 정상 60분 구간 1개와 duplicate/late/invalid/spike 시나리오 | 작은 fixture는 Git, 큰 capture는 로컬/OCI volume | 장외 데모, 1x·10x·50x·100x 부하 테스트, 장애 회귀 테스트 | 작은 deterministic fixture는 계속 보존. 임시 live raw capture는 최종 발표 30일 후 삭제 |
| DLQ | validation 실패 또는 처리 불가 event | Kafka `dead-letter.v1`; 필요 시 오류 metadata만 PostgreSQL | 오류 유형·건수 확인과 재현 | 7일 후 자동 삭제 |
| Pipeline log/metric | 실행 중 structured log와 run별 집계 metric | 각 node log volume, 결과 요약은 report | lag, latency, CPU/RAM, recovery time 설명 | 원본 log 14일, 요약 report는 repository에 계속 보존 |
| News metadata — 선택 | 22종목 관련 기사만, 구현 시 수집 | PostgreSQL metadata/event tables | alert 주변의 관련 기사 후보 조회 | metadata 30일. 기사 전체 본문은 저장하지 않음 |

SIP DAG의 `now-20m`은 무료 historical SIP의 15분 제한에 5분 safety margin을 둔 초기값이다. 실제 account smoke test 후 schedule을 조정해도 `end <= now-15m` 계약은 위반하지 않는다.

## 3. Feature와 분석에서 실제로 사용하는 방법

### 과거 데이터 재생과 백테스트의 경계

이번 단계에서 과거 데이터를 쓰는 목적은 두 가지이며 처리 경로를 섞지 않는다.

```text
파이프라인 검증용 historical trade / fixture
→ WebSocket과 같은 raw trade schema
→ Kafka raw.market.v1 → Spark → PostgreSQL
→ 중복·순서 역전·지연·재시작·배속 부하 검증

경제지표 분석용 historical SIP 1-minute bar
→ Airflow batch backfill → PostgreSQL
→ 공식 발표 시각·당시 vintage와 결합
→ event-study backtest
```

SIP bar를 raw trade처럼 가장해 Kafka에 넣지 않는다. 반대로 Kafka→Spark의 장애 복구를 검증할 때는 이미 집계된 bar가 아니라 실제와 같은 raw trade 계약을 사용한다. 이번 `event-study backtest`는 과거 발표에 대해 분석 결과가 재현되는지 확인하는 것이며, 가상 예산으로 매수·매도 수익률을 계산하는 strategy/portfolio backtest는 후속 단계다.

### 경제지표 발표 영향 분석 — 첫 번째 목표

```text
공식 발표 시각 + 당시 이용 가능했던 FRED/ALFRED vintage
→ SIP 발표 전후 1분 bar
→ 5분·30분·60분 수익률, 거래량, 실현 변동성
→ 평소 같은 시간대의 비발표일 baseline과 비교
→ SPY/QQQ와 섹터 ETF 대비 반응 비교
→ 동일 발표 유형의 여러 날짜를 집계
→ 관측된 연관성, 표본 수, coverage와 한계 저장
```

초기 event type은 CPI, Employment Situation, FOMC다. 하나의 날짜만으로 “이 지표 때문에 움직였다”고 결론 내리지 않는다. 장전 data coverage가 기준보다 낮으면 `PARTIAL_MARKET_COVERAGE`, 반복 표본이 부족하면 `INSUFFICIENT_EVENT_SAMPLES`로 표시한다.

발표 시점과 장 시작 중 하나만 임의로 고르지 않는다. 데이터가 허용하면 아래 구간을 별도 결과로 저장하고 서로 섞지 않는다.

- 발표 전: `released_at` 이전 60분부터 5분 전까지의 변화
- 발표 직후: `released_at` 기준 5분·30분·60분 반응
- 첫 정규장: 해당 발표 뒤 첫 정규장 시작 기준 5분·30분·60분 반응

발표 전 움직임은 관측 가능한 `pre-event drift/anticipation`으로만 표현한다. 사람들의 공포·기대 같은 심리 상태는 별도 설문·뉴스·포지셔닝 데이터가 없으면 결론 내리지 않는다.

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

### FRED/ALFRED 값과 지속적인 시장 환경

| Series | 사용할 값 | 해석 범위 |
| --- | --- | --- |
| `DGS2`, `DGS10` | 최신 이용 가능 수준, 전 관측일 변화, `DGS10-DGS2` | 단기·장기 금리와 장단기 금리차 환경 |
| `DFF` | 최신 이용 가능 수준과 변화 | 정책금리 환경 |
| `CPIAUCSL`, `CPILFESL` | 최근 발표값과 직전 발표 대비 변화 | 소비자물가 환경 |
| `PCEPI`, `PCEPILFE` | 최근 발표값과 직전 발표 대비 변화 | PCE 물가 환경 |
| `UNRATE` | 최근 발표값과 직전 발표 대비 변화 | 고용 환경 |
| `VIXCLS` | 최신 이용 가능한 수준과 변화 | 시장 변동성 환경 |

경제 이벤트에는 해당 발표 시점에 이용 가능했던 값과 vintage만 연결한다. `DGS2`, `DGS10`, `DFF`, `VIXCLS` 같은 연속 환경 지표도 event 이전 최신값만 사용한다. 미래 revision을 섞지 않으며 관측된 반응을 인과관계로 단정하지 않는다.

## 4. 최종 조회 결과

첫 번째 결과는 경제지표 영향 report다.

```text
event type, reference period, official released_at와 source URL
+ 당시 공개된 actual/previous와 vintage
+ 종목별 발표 후 5분·30분·60분 수익률·거래량·변동성
+ 평소 같은 시간대와 시장·섹터 ETF 대비 차이
+ 같은 발표 유형의 과거 표본 수와 분포
+ data coverage, analysis version과 해석 한계
```

두 번째 결과는 한 실시간 alert의 설명이다.

```text
alert id, symbol, event time, status
+ IEX 1분 bar와 return/volume/ATR feature
+ 사용한 IEX threshold와 baseline version
+ SIP 동일 구간 feature와 reconciliation 결과
+ SPY/QQQ/SMH/SOXX 동일 구간 변화
+ alert 시각 기준 최신 macro 환경과 가까운 공식 경제 이벤트
+ 데이터 freshness와 pipeline 상태
```

이 결과는 Stage A에서 주문 지시가 아니라 **무엇을 관측했고, 어느 범위의 데이터로 검증했는지**를 보여주는 전략 입력 기반이다. 매수·매도 결정은 후속 전략, 거래비용을 포함한 백테스트와 위험 관리 규칙의 책임이다.

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

OCI를 사용할 경우 Kafka/PostgreSQL/Airflow/Grafana는 공용 인터넷에 직접 노출하지 않고 private IP 또는 SSH tunnel을 사용한다. NSG와 host firewall은 필요한 node 간 통신과 관리 IP의 SSH만 허용한다. 배포 완료의 기준에는 PostgreSQL dump 생성뿐 아니라 빈 instance/volume에서의 restore smoke test가 포함된다.

각 외부 API가 제공하는 raw field와 실제 선택 범위는 [API 데이터 소스 카탈로그](data-source-catalog.md)를 따른다.

## 6. 측정 후 바꿀 수 있는 항목

다음 값은 초기 정책이지만 조용히 변경하지 않는다.

- 정확한 22개 symbol allowlist
- live event/byte 수와 Kafka disk 사용량
- volume baseline window와 alert threshold
- watermark delay와 SIP safety margin
- PostgreSQL 90일 retention이 실제 disk에 미치는 영향
- OCI node별 container memory limit

변경 시 측정 결과, 이전 값, 새 값, 변경 이유를 load-test report 또는 ADR에 기록한다.
