# 6차시 실행 증거

이 폴더에는 공개해도 되는 실행 요약만 둡니다. API 키, DB 접속 문자열, 원시 가격 데이터는 포함하지 않습니다.

- `alert-failure.json`: 외부 API를 호출하지 않는 모의 503에서 작업이 `FAILED`, 알림이 `OPEN`이 된 결과
- `alert-recovery.json`: 커밋된 합성 1분봉 fixture로 한 번 재시도해 작업이 `SUCCEEDED`, 알림이 `RESOLVED`가 된 결과
- `integrity.txt`: 같은 business key를 Upsert한 뒤 중복이 없는지 조회한 결과
- `airflow-run.json`: 실제 Alpaca SIP를 사용한 FOMC 1회 × SPY·TLT Airflow 실행 2회의 요약
- `airflow-task-states.txt`: 두 실행의 종목별 작업 상태와 처리 건수
- `postgres-summary.txt`: 정확한 588개 결과 범위와 재실행 후 PostgreSQL 해시

이 장애 실험의 합성 데이터는 파이프라인의 장애·복구 제어만 검증합니다. Alpaca 실제 데이터 수집 증거로 사용하지 않습니다. 실제 provider-bar 실행 결과는 별도의 Airflow 실행 증거에 기록합니다.

실제 Airflow 실행은 `2026-09-03`에 수행했습니다. 그중 마지막 두 실행을 동일 입력 재실행 비교에 사용했습니다. 두 실행 모두 작업 2개가 성공했고 미해결 알림은 0개였습니다. 비교 실행 전과 재실행 후의 정확한 결과 범위는 모두 588행, 내용 해시는 모두 `ee58892b2b1c6fab311d773b32722f52`, business key 중복은 0건이었습니다.
