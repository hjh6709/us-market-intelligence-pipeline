# 서빙 레이어 실제 실행 증거

이 폴더의 파일은 2026-09-04에 로컬 PostgreSQL에 저장된 결과를 실제로 읽어 만든 공개 가능한 증거다. API 키, DB 연결 문자열, 원시 체결 Parquet은 포함하지 않았다.

## 실행 입력

- 경제 발표: `CPI|2026-07|2026-08-12T12:30:00Z`
- 종목: `NVDA`
- 데이터 출처: PostgreSQL의 `alpaca` / `sip`
- 실행 명령:

```bash
.venv/bin/python scripts/run_serving_demo.py \
  --event-id 'CPI|2026-07|2026-08-12T12:30:00Z' \
  --symbol NVDA \
  --output docs/evidence/serving-layer/demo-result.json
```

`/usr/bin/time -p`로 측정한 실제 실행 시간은 `0.30초`였다. 이 명령은 이미 저장된 1분봉으로 선택 발표의 영향과 전략 결과를 다시 계산하고 Upsert한 뒤, 같은 결과를 서빙 계층으로 다시 읽는다. Alpaca·FRED·ALFRED 같은 외부 API와 증권사 주문 API는 호출하지 않았다.

## 확인 결과

| 단계 | 실제 결과 |
|---|---:|
| 입력 | 발표 1회 × 종목 1개 |
| 처리 | 발표 영향 4개 구간 |
| 저장 | 전략 결과 1행 Upsert |
| 읽기 | 영향 4행, 1분·3분·5분봉 |
| 중복 고유키 | 0 |
| 최종 운영 단계 | `RESEARCH_ONLY` |
| 실제 주문 행동 | `NO_TRADE` |

같은 명령을 연속 두 번 실행한 뒤에도 선택 입력의 `macro_event_impacts`는 4개, `event_strategy_results`는 1개였고 전체 중복 고유키는 모두 0이었다.

## 파일 설명

- `demo-result.json`: 입력 → 처리 → 저장 → 읽기의 기계 판독 가능한 결과
- `api-detail.json`: 같은 발표·종목의 실제 상세 API 응답
- `dashboard.png`: 1440×1000 화면에서 실제 API를 읽은 대시보드 캡처

캡처에서 선택 사례의 연구 신호는 `LONG`, 과거 시뮬레이션 순수익률은 `0.47785058%`다. 그러나 전체 전략 평균은 약 `-0.15649%`이고 전망치 대비 실제값, 모의주문, 포지션 복구, 긴급 중지가 준비되지 않았다. 따라서 이 사례의 실제 주문 행동도 `NO_TRADE`다.
