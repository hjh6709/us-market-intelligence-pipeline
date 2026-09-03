# 6차시 전체 흐름 점검 증거

이 폴더에는 5차시 실행 결과를 다시 꾸미지 않고, 6차시 점검에서 새로 확인하고 수정한 내용만 보관합니다.

## 가장 중요한 보완

5차시 문서의 `Spark 중복 49건`은 실제 중복이 아니었습니다. Alpaca의 거래 ID `i`는 거래소가 다르면 같은 시각에 다시 사용될 수 있는데, 기존 `event_id`가 거래소 `x`를 포함하지 않아 서로 다른 체결을 같은 것으로 판단했습니다.

```text
기존: source + feed + type + symbol + trade_id + timestamp
수정: source + feed + type + symbol + exchange + trade_id + timestamp
```

수정 후 저장된 Parquet 7,360,804건 전체를 다시 검사하고 Kafka → Spark → PostgreSQL 경로도 다시 실행했습니다.

| 확인 항목 | 결과 |
| --- | ---: |
| 전체 원시 체결 | 7,360,804 |
| 기존 식별키 충돌 | 49 |
| 수정 식별키 충돌 | 0 |
| Kafka 발행 / 수신 | 7,360,804 / 7,360,804 |
| Spark 입력 / invalid / duplicate | 7,360,804 / 0 / 0 |
| PostgreSQL 1분봉 / 고유키 중복 | 22,260 / 0 |
| 기준 입력 재실행 후 전체 행 / hash 변경 | 22,260 / 변경 없음 |

## 파일 설명

- [`event-identity-correction.json`](event-identity-correction.json): 원인과 수정 전·후 식별키 전수 검사
- [`corrected-load-run.json`](corrected-load-run.json): 수정 코드로 전체 데이터를 다시 처리한 결과
- [`corrected-repeat-run.json`](corrected-repeat-run.json): 기준 입력 118,118건을 다시 Upsert한 뒤 전체 행과 hash가 유지된 결과
- [`corrected-integrity.txt`](corrected-integrity.txt): 최종 DB 행 수·고유키·결과 hash·경제지표 시점 검사

5차시 GCP JSON은 당시 실행 원본이므로 수정하지 않았습니다. 그 파일의 `spark_duplicates=49`는 당시 코드가 출력한 값이라는 실행 증거이고, 현재 데이터의 진짜 중복 수로 해석하면 안 됩니다. 현재 정본은 이 폴더의 수정 후 결과입니다.
