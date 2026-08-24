# 아키텍처 다이어그램

이 디렉터리는 README와 발표에서 사용하는 데이터 파이프라인 다이어그램을 관리한다.

| 파일 | 용도 |
| --- | --- |
| `pipeline-architecture.svg` | 전체 프로젝트 정본. 현재 구현된 두 데이터 경로와 후속 확장을 구분 |
| `pipeline-architecture.png` | 루트 README에서 사용하는 전체 프로젝트 렌더 결과 |
| `cpi-sip-kafka-spark-assignment.svg` | 한 CPI 발표일의 Kafka·Spark 과제 실행 정본 |
| `cpi-sip-kafka-spark-assignment.png` | 과제 문서와 발표에서 사용하는 실행 결과 렌더 |

다이어그램은 기술 목록이 아니라 다음 데이터 이동 순서를 기준으로 읽는다.

```text
Data Source
→ Ingestion
→ Raw Data Storage
→ Processing
→ Processed Storage
→ Analysis / BI
```

- 파란색·초록색·주황색 실선: 현재 구현하고 실행 결과를 검증한 경로
- 보라색 점선: Airflow 자동화, API·Dashboard와 strategy backtest 등 후속 경로
- 루트 README에는 전체 프로젝트 그림만 배치하고, 과제 실행 그림은 과제 문서에서만 사용한다.

PNG를 수정한 뒤에는 SVG 정본과 내용이 같은지, 글자가 잘리거나 lane 경계를 침범하지 않는지 확인한다.
