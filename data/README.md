# 데이터 파일 안내

이 디렉터리는 공개 가능한 형식 예시와 로컬에서만 보관하는 실제 수집 데이터를 분리한다.

| 위치 | 데이터 | Git 공개 |
| --- | --- | --- |
| `sample/market_bars.synthetic.csv` | PostgreSQL `market_bars` 형식을 설명하는 합성 예시 | 포함 |
| `local/actual_market_bars.csv` | 실제 Alpaca 거래를 처리해 만든 1분 봉 | 제외 |
| `raw/` | 원본 API 응답이나 임시 원천 파일 | 제외 |

`sample/market_bars.synthetic.csv`는 실행 증거가 아니라 스키마를 이해하고 테스트하기 위한 예시다. 실제 데이터 처리 결과는 PostgreSQL에 있으며, 다음 명령으로 로컬 파일을 만들 수 있다.

```bash
.venv/bin/python -m scripts.evidence.export_actual_market_bars
```

명령은 `SMH`, `2026-08-19T19:50:00Z` 이상 `19:56:00Z` 미만의 실제 저장 봉을 `data/local/actual_market_bars.csv`로 내보내고 행 수·시간 범위·SHA-256 해시를 출력한다. 해시는 커밋된 `result.json`과 자동 대조하며 다르면 종료 코드 3으로 실패한다. 출력 경로는 `data/local/` 아래만 허용되고 이 디렉터리는 `.gitignore` 대상이다.

## 실제 값을 공개 저장소에 넣지 않는 이유

Alpaca는 Market Data API로 얻은 데이터를 별도 허가 없이 재배포하지 않도록 안내한다. 따라서 공개 저장소에는 원본 payload와 정확한 시장 가격 행을 올리지 않고, 수집 건수·저장 건수·시간 범위·중복 검사와 재현 명령만 남긴다.

- [Alpaca: Can I redistribute Alpaca API data?](https://alpaca.markets/support/redistribute-alpaca-api)
- [실제 수집·저장 테스트 보고서](../docs/test-results/2026-08-20-actual-ingestion.md)
- [기계 판독용 검증 결과](../docs/evidence/actual-ingestion/result.json)

정확한 행 값은 발표 시 로컬 PostgreSQL 조회 화면으로 보여주고 공개 Git에는 커밋하지 않는다. 공개된 해시는 같은 로컬 행인지 확인하는 무결성·일관성 증거이며, 데이터가 Alpaca에서 왔다는 사실을 해시만으로 독립 증명하는 것은 아니다. 실제 데이터 파일 제출이 필수라면 재배포가 허용된 데이터셋을 별도로 선택하거나 제공자의 허가를 받아야 한다.
