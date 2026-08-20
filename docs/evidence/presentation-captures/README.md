# 발표용 실행 증거 캡처

과제 발표에서 코드 존재 여부가 아니라 실제 데이터가 수집·처리·저장됐음을 빠르게 보여주기 위한 1280×720 캡처다.

모든 이미지는 저장소의 테스트 보고서, 집계 결과 JSON과 2026-08-20의 읽기 전용 상태 조회를 바탕으로 작성했다. API key, secret, connection URL, `.env`, 원본 API payload는 포함하지 않았다. 실제 터미널 전체 화면은 비밀정보 노출 가능성이 있어 Git에 올리지 않는다.

| 파일 | 보여주는 내용 | 근거 |
| --- | --- | --- |
| `01_services_and_raw_storage.png` | Kafka·PostgreSQL healthy, Kafka partition offset | 로컬 서비스와 Kafka read-only 조회 |
| `02_live_websocket_kafka.png` | 실제 IEX WebSocket 10건 수신·발행·재소비 | [WebSocket 테스트 보고서](../../test-results/2026-08-19-kafka-producer-smoke.md) |
| `03_actual_ingestion_e2e.png` | Historical 실제 거래 427건 → final 1분봉 3건 | [실제 ingestion 보고서](../../test-results/2026-08-20-actual-ingestion.md) |
| `04_postgres_storage.png` | SMH 3개 봉, 거래 174건, 거래량 6,914, 중복 0 | PostgreSQL read-only 집계와 [result.json](../actual-ingestion/result.json) |
| `05_automated_tests.png` | 단위 테스트 45개와 Kafka·Spark 통합 테스트 | 2026-08-20 재실행 결과 |
| `06_database_recovery.png` | DB 중단 실패 기록과 복구 후 동일 batch 재처리 | [PostgreSQL 결과](../postgres-market-bars/result.json) |

발표에서는 1번으로 서비스 상태, 2번으로 실시간 수집, 3·4번으로 실제 저장, 5·6번으로 품질과 장애 대응을 보여준다.
