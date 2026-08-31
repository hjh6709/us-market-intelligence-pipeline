# 부하·장애·복구 실행 증거

이 폴더에는 2026-08-31에 실제로 실행한 결과 중 공개 저장소에 올려도 되는 집계값만 보관합니다. API key, DB 접속 문자열, 원시 거래 가격과 대용량 Parquet은 포함하지 않습니다.

- [`results.json`](results.json): 데이터 범위, GCP 사양, 기준·부하·DB 장애·복구 결과
- [`gcp-baseline.json`](gcp-baseline.json): GCP 기준 실행의 비밀정보 제거 원본 결과
- [`gcp-load.json`](gcp-load.json): GCP 7,360,804건 부하 실행의 비밀정보 제거 원본 결과
- [`gcp-db-failure.json`](gcp-db-failure.json): PostgreSQL 중단 실행의 실패 결과
- [`gcp-db-recovered.json`](gcp-db-recovered.json): PostgreSQL 복구 후 동일 입력 재실행 결과
- [`local-safe-faults.json`](local-safe-faults.json): mock API 503·잘못된 입력·DB endpoint 오류 결과
- [`local-idempotency-review.json`](local-idempotency-review.json): 같은 기준 입력 재실행 결과
- [`macro-daily-cutoff.txt`](macro-daily-cutoff.txt): 일별 금리·VIX의 발표 당일 값 제외 재수집·SQL 검증
- [`integrity.txt`](integrity.txt): 장애 전후 DB 행 수와 고유키 중복 확인
- [`01-baseline-vs-load.png`](01-baseline-vs-load.png): 기준과 부하 실행 비교 화면
- [`02-failure-and-recovery.png`](02-failure-and-recovery.png): PostgreSQL 장애와 복구 결과 화면
- [`03-data-scope-and-integrity.png`](03-data-scope-and-integrity.png): 데이터·경제지표 범위와 최종 무결성 화면
- [`source.html`](source.html): 위 캡처를 만든 정적 요약 화면. 캡처는 실행 원본 자체가 아니며, 수치의 원본은 위 개별 JSON과 `integrity.txt`입니다.

전체 원본은 로컬 `data/archive/`에 55개 CPI 발표 × 4종목 = 220개 Parquet 파티션으로 저장되며 Git에서 제외됩니다. GCP VM은 결과 JSON을 내려받은 뒤 추가 비용을 막기 위해 삭제했습니다.
