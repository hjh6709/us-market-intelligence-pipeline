# 아키텍처 다이어그램

이 디렉터리는 README와 발표에서 사용하는 데이터 파이프라인 다이어그램을 관리한다.

| 파일 | 용도 |
| --- | --- |
| `pipeline-architecture.svg` | 편집 가능한 정본. 텍스트, 색상, 현재/예정 흐름을 수정할 때 사용 |
| `pipeline-architecture.png` | GitHub README와 발표에서 바로 확인하는 렌더 결과 |

다이어그램은 기술 목록이 아니라 다음 데이터 이동 순서를 기준으로 읽는다.

```text
Data Source
→ Ingestion
→ Raw Data Storage
→ Processing
→ Processed Storage
→ Analysis / BI
```

- 파란색·초록색 실선: 현재 코드와 실행 증거가 있는 경로
- 보라색 점선: 다음 회차에서 구현할 배치·분석 경로
- 현재 경로: Alpaca → Python Ingestion → Kafka → Spark → PostgreSQL → SQL 검증
- 다음 경로: 공식 경제지표·FRED·Historical SIP → Airflow → 영향 분석 → BI

PNG를 수정한 뒤에는 SVG 정본과 내용이 같은지, 글자가 잘리거나 lane 경계를 침범하지 않는지 확인한다.
