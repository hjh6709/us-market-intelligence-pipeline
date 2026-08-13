# 과정 학습 내용과 프로젝트 구현 연결

이 문서는 데이터 엔지니어 과정에서 학습·실습한 기술을 현재 프로젝트에서 **어떤 문제를 해결하는 데 다시 사용하는지**, 반대로 **왜 사용하지 않는 기술이 있는지**를 설명한다. 목표는 기술 이름을 많이 나열하는 것이 아니라, 배운 내용을 하나의 재현 가능한 주식 데이터 파이프라인에서 구현하고 검증하는 것이다.

## 1. 적용 원칙

1. Kafka, Spark Structured Streaming, Airflow는 과정 필수 기술이므로 Stage A MVP에서 직접 구현한다.
2. PostgreSQL, Docker Compose, load/failure test는 파이프라인을 실행하고 결과를 검증하는 필수 경계로 사용한다.
3. Prometheus/Grafana는 항상 실행하는 서비스가 아니라 6회차 부하·장애 검증용 `monitoring` profile로 사용한다.
4. Kafka Connect, AWS 관리형 서비스, Kubernetes는 현재 데이터 흐름에 필요할 때만 도입한다.
5. 이미 별도 과제로 구현한 기능을 그대로 복제하기보다, 이번 프로젝트의 데이터·장애 시나리오에 맞게 재검증한다.

## 2. 학습 내용 → 프로젝트 구현 → 증거

| 학습·실습 범위 | 현재 프로젝트에서의 구현 | 최종 발표 증거 |
| --- | --- | --- |
| Kafka topic, partition, key, retention | `raw.market.v1`, key=`symbol`, 초기 3 partitions, 24시간 retention | topic describe, symbol별 partition 분포, 실제 disk 사용량 |
| Producer `acks`, retry, 멱등성 | `confluent-kafka-python`, delivery callback, `enable.idempotence=true`, `acks=all`, bounded reconnect | 성공·실패 callback log, 재연결·중복 fixture 결과 |
| Consumer offset, lag, replay | Spark checkpoint와 Kafka offset, deterministic replay producer | checkpoint 재시작 전후 offset·row 비교 |
| Kafka Connect Source/Sink | Alpaca WebSocket에는 직접 Producer 사용. 장기 archive가 필요할 때만 별도 Sink 검토 | Connect를 사용하지 않은 이유와 adapter 경계 설명 |
| Spark schema와 DataFrame | provider raw JSON parsing, normalized `MarketTrade`, validation/DLQ | 입력 JSON과 normalized row, invalid code |
| Structured Streaming window·watermark | event-time 1분 OHLCV·VWAP·trade count, late-event 처리 | 정상·중복·허용 지연·초과 지연 fixture 결과 |
| Spark checkpoint·장애 복구 | query checkpoint와 PostgreSQL idempotent upsert 분리 | Spark 재시작 및 DB 장애 후 row/value 일치 |
| Airflow TaskFlow·의존성 | FRED와 SIP reconciliation의 extract → validate → upsert → quality 흐름 | Graph/Grid View, task log와 logical date |
| Airflow retry·backfill·멱등성 | timeout, exponential backoff, overlap 조회, business-key upsert | 동일 logical date 재실행·backfill 중복 없음 |
| Dynamic Task Mapping | 9개 FRED series를 독립 호출할 실익이 확인될 때만 적용 | 적용 시 mapped task별 성공·실패, 미적용 시 결정 근거 |
| Dataset 기반 DAG | 별도 데이터 준비 완료 이벤트로 DAG를 분리할 때만 적용 | 단일 DAG보다 명확한 의존성이 생길 때 ADR 작성 |
| PostgreSQL·SQL | feed별 bar/feature, alert 상태, reconciliation, macro 저장 | schema, unique key, index와 주요 SQL query |
| Prometheus/Grafana | 6회차 `monitoring` profile에서 Kafka/Spark/app metric 수집 | 처리량·lag·batch duration·장애 시계열 dashboard |
| Kafka broker 장애·ISR | 기본 single broker는 재시작 복구만 검증. 선택 `resilience` profile에서 3-broker ISR 실험 | 기본 환경의 HA 비보장 명시, 선택 실험의 leader/ISR 변화 |
| Docker Compose | `core`, `batch`, `monitoring`, `optional-app`, 선택 `resilience` profile | clean checkout 실행 명령과 container health |
| AWS IAM·S3·Redshift·Glue | AWS를 억지로 재사용하지 않고 최소 권한·비밀 관리·columnar/partition 원칙만 계승 | OCI/local 선택 근거와 보안 경계 |
| Docker/Kubernetes | Docker Compose까지만 P0. Kubernetes는 현재 처리량과 기간에서 제외 | 제외 이유와 도입 조건 |

## 3. 10주차 실습에서 이어지는 부분

기존 종합 실습에서는 JDBC Source, Spark 스트리밍 조인·이상 감지, Kafka 모니터링, Producer 튜닝, 다중 브로커 장애, S3 Sink, Redshift·Spectrum, Glue Spark ETL을 수행했다. 이번 프로젝트는 그중 다음 능력을 하나의 시장 데이터 흐름으로 연결한다.

```text
기존의 센서 JSON schema·이상 판정
→ 시장 trade schema·1분 OHLCV·가격/거래량 이상 징후

기존의 batch/linger/compression 튜닝
→ 실제 replay 배속별 throughput·latency·lag 측정

기존의 broker 장애와 Grafana 관찰
→ Spark/DB/Kafka 장애 후 checkpoint·upsert 복구 검증

기존의 AWS batch ETL
→ Airflow logical date·backfill·quality check가 있는 SIP/FRED batch
```

이번 프로젝트의 차별점은 같은 예제를 반복하는 것이 아니라, **실시간 탐지와 지연 검증을 분리하고 전체 처리 이력을 데이터 모델로 남기는 것**이다.

## 4. 의도적으로 사용하지 않는 기술

### Kafka Connect를 Alpaca 수집에 사용하지 않는다

Alpaca는 WebSocket 인증, 종목 subscribe, heartbeat, reconnect와 provider별 payload 처리가 필요하다. 일반 JDBC Source처럼 설정만으로 해결되는 입력이 아니므로 직접 Producer와 provider adapter가 더 명확하다. Connect는 raw archive나 외부 sink 요구가 실제로 생길 때 다시 검토한다.

### Kubernetes를 사용하지 않는다

4주 MVP는 local Docker Compose와 최대 OCI A1 소형 인스턴스가 대상이다. Kubernetes를 추가해도 현재 병목이나 가용성 요구를 해결하지 않으며 운영 요소만 증가한다. 다중 노드 배포·자동 복구·독립 확장이 실제 요구가 될 때 후속 단계에서 검토한다.

### AWS 관리형 서비스를 필수로 사용하지 않는다

S3, Redshift, Spectrum, Glue는 기존 실습으로 경험했다. 이번 프로젝트는 무료·재현 가능한 local/OCI 구성을 우선하며 PostgreSQL이 P0 query를 충족하는지 먼저 검증한다. 장기 raw archive나 대규모 분석 요구가 측정되면 Parquet/object storage를 추가한다.

### Airflow 고급 기능을 체크리스트처럼 넣지 않는다

Dynamic Task Mapping과 Dataset scheduling은 문제를 단순하게 만들 때만 사용한다. 9개 FRED series의 독립 실패 격리가 필요하면 mapping을 적용할 수 있지만, 불필요한 task 수와 API 호출을 늘린다면 단일 extract task를 유지한다.

## 5. 인프라별 검증 경계

| 환경 | 목적 | 포함 | 보장하지 않는 것 |
| --- | --- | --- | --- |
| 로컬 `core` | 첫 수직 슬라이스와 회귀 테스트 | Kafka single broker, Spark local, PostgreSQL, replay | broker HA |
| 로컬 `batch` | SIP/FRED workflow | Airflow, PostgreSQL | 초 단위 실시간 처리 |
| 로컬 `monitoring` | 6회차 측정·발표 | Prometheus, Grafana, exporters | 상시 운영 |
| 로컬 `resilience` — 선택 | Kafka 복제·ISR 장애 실험 | 3 KRaft brokers, replication factor ≥ 2 | OCI 6GB 실행 가능성 |
| OCI A1 — 제안 | 무료 원격 배포 smoke test | 측정 후 선택한 최소 profile | local과 같은 동시 실행 규모, managed HA |

## 6. 과정 성과를 증명하는 최종 산출물

- Kafka producer 설정과 delivery 결과
- topic/partition/key/retention 결정 및 측정값
- Spark schema, window, watermark, checkpoint 코드
- PostgreSQL unique key와 재실행 결과
- Airflow DAG, retry/backoff, backfill 결과
- Prometheus/Grafana 부하·장애 dashboard
- 1x·10x·50x·100x load-test report
- Spark·DB·Kafka failure recovery report
- single-broker와 선택 multi-broker의 보장 범위 비교
- OCI를 사용했다면 ARM64·메모리·NSG·backup 검증 결과

이 산출물에 코드, 실행 로그, 측정값이 함께 있어야 “배웠다”가 아니라 “직접 구현하고 설명할 수 있다”는 근거가 된다.
