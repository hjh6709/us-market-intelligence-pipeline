# 6차시 실행 증거

이 폴더에는 공개해도 되는 실행 요약만 둡니다. API 키, DB 접속 문자열, 원시 가격 데이터는 포함하지 않습니다.

- `alert-failure.json`: 외부 API를 호출하지 않는 모의 503에서 작업이 `FAILED`, 알림이 `OPEN`이 된 결과
- `alert-recovery.json`: 커밋된 합성 1분봉 fixture로 한 번 재시도해 작업이 `SUCCEEDED`, 알림이 `RESOLVED`가 된 결과
- `integrity.txt`: 같은 business key를 Upsert한 뒤 중복이 없는지 조회한 결과

이 장애 실험의 합성 데이터는 파이프라인의 장애·복구 제어만 검증합니다. Alpaca 실제 데이터 수집 증거로 사용하지 않습니다. 실제 provider-bar 실행 결과는 별도의 Airflow 실행 증거에 기록합니다.
