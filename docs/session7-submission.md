# 7차시 제출 안내

## 제출할 링크

- 저장소: [us-market-intelligence-pipeline](https://github.com/hjh6709/us-market-intelligence-pipeline)
- 발표 자료: [7차시 서빙 레이어와 최종 발표](https://github.com/hjh6709/us-market-intelligence-pipeline/blob/main/docs/serving-layer-assignment.md)
- 최신 구성도: [Archify 대화형 HTML](diagrams/session7-architecture.html)
- 발표 대본: [09.07 7차시 발표 대본](09.07_대본.md)
- 실제 단일 실행 기록: [바로 읽기](evidence/session7-demo/session7-actual-e2e.txt) · [재생 원본](evidence/session7-demo/session7-actual-e2e.typescript)
- 실제 저장 결과 조회 영상: [65초 MP4](evidence/session7-demo/session7-live-dashboard.mp4)
- 70초 보조 영상: [MP4](evidence/session7-demo/session7-submission-demo.mp4) · [한국어 자막 트랙 MP4](evidence/session7-demo/session7-submission-demo-captioned.mp4) · [WebM 원본](evidence/session7-demo/session7-submission-demo.webm)

별도 슬라이드 대신 위 발표 문서를 사용한다. 디스코드 전송은 제출자 계정에서 직접 한다.

## 요구사항별 완료 상태

| 요구사항 | 상태 | 실제 결과 | 증거 |
|---|---|---|---|
| 저장 결과를 읽는 장면 | PASS | 실행 중인 FastAPI에서 CPI·NVDA 상세·1m 봉 HTTP 200, 대시보드 조회 성공 | [65초 실제 화면 녹화](evidence/session7-demo/session7-live-dashboard.mp4), [실제 화면](evidence/serving-layer/dashboard-cpi-nvda-live-20260908.png), [최종 HTTP 증거](evidence/serving-layer/final-verification-20260907.json) |
| 입력 → 처리 → 저장 → 읽기 | PASS | 실제 명령 1회: 1조합 → 영향 4행 → 전략 1행 Upsert → 영향 4행·1m/3m/5m 재조회, 중복 0 | [읽기용 출력](evidence/session7-demo/session7-actual-e2e.txt), [재생 가능한 원본](evidence/session7-demo/session7-actual-e2e.typescript) |
| 1~2분 안전한 시연 | PASS | 실제 CLI는 1초 이내이며, 동일 명령을 재생 가능한 원본으로 보존 | [터미널 출력](evidence/session7-demo/session7-actual-e2e.txt), [65초 실제 브라우저 녹화](evidence/session7-demo/session7-live-dashboard.mp4) |
| 최신 구성도 | PASS | merged main `7f55721`의 소스 참조 12개를 Archify가 검증, showcase 9/9 | [HTML](diagrams/session7-architecture.html), [자동 검증 receipt](diagrams/session7-architecture.visual-check.json), [수동 시각 검토](diagrams/session7-architecture.manual-review.json) |
| 단계별·최종 건수 표 | PASS | 202 releases, 10 symbols, 2,020 work items, 8,080 impacts 등 계층별 표기 | [발표 문서](serving-layer-assignment.md) |
| 부하·장애·복구와 한계 | PASS | 실제 체결 부하 범위와 202×10 bar 범위를 분리하고 미보장 항목 표기 | [발표 문서 4절](serving-layer-assignment.md#4-부하장애복구에서-확인한-것) |
| README 실행법·구성·확인법 | PASS | 로컬 실행, API URL, 시연 명령, 증거 링크 반영 | [README](../README.md) |
| 코드에 없는 기능 구분 | PASS | 자동주문·위험관리·사람 승인·odd-lot 비교는 후속 계획으로 표기 | [발표 문서 7절](serving-layer-assignment.md#7-남은-문제와-다음-단계) |

## 제출 숫자 정본

| 구분 | 실제 값 | 해석 |
|---|---:|---|
| 공식 발표 / 종목 / work item | 202 / 10 / 2,020 | 분석 범위 |
| 공급자 요청·페이지 | 404 / 404 | 발표별 1m·1d 다종목 요청; 추가 페이지 없음 |
| 이벤트별 선택 합계 | 1m 308,512 / daily 30,270 | 인접 이벤트가 같은 DB 행을 공유할 수 있음 |
| 파생 봉 | 3m 112,593 / 5m 70,090 | 각각 PARTIAL 19,178 / 16,215 |
| PostgreSQL 고유 시장 봉 | 1m 308,512 / 3m 112,593 / 5m 70,090 / 1d 11,700 | 2026-09-08 재수집 후 business key 기준 실제 저장 행 |
| 경제 환경 | 2,020 | 202 events × 10 series |
| 이벤트 영향 | 8,080 | COMPLETE 5,366, PARTIAL 2,557, NO_MARKET_DATA 152, baseline 부족 5 |
| 탐색 전략 | 2,020 중 계산 가능 1,988 | 전체 평균 -0.15649%, COMPLETE 입력 911 |

위 숫자는 행의 의미가 다르므로 합쳐서 하나의 “전체 원본 건수”로 말하지 않는다.

## Coverage 계약 주의사항

PR #28은 `main`의 `7f55721ddcfea021487664429a188776465ee0d4`로 병합됐다. 이제 공급자 요청 완료 여부와 실제 가격 봉 밀도를 분리한다.

- 수집 완료: HTTP 성공, 요청 범위, pagination 종료, truncation·provider failure·off-grid 오류가 없는지로 판단한다.
- 관측 coverage: 반환된 가격 봉 수와 파생 bucket의 `source_bar_count/expected_bar_count`를 품질 정보로 보존한다.
- 정상적인 sparse bar만으로 수집 실패 처리하지 않고 가격을 forward fill하지 않는다.
- macro analysis의 기존 90% 규칙은 별도 분석 사용성 정책이며 변경하지 않았다.

202×10 전체 Airflow 증거의 `COMPLETE 1,980` 분류는 PR #28 이전 계약이므로 과거 실행 기록으로만 보존한다. 2026-09-08 현재 코드의 전체 재수집에서는 session collection COMPLETE 2,020, daily collection COMPLETE 2,010·PARTIAL 10으로 확인했다. 관측 품질은 session 1m COMPLETE 581·PARTIAL 1,409·NO_DATA 30, daily COMPLETE 1,990·PARTIAL 30이다. 기존 90% 분석 규칙과 전략 로직으로 영향 8,080행과 전략 2,020행도 다시 계산했고 결과 중복은 0건이다.

## 180분과 181개 후보 시각

- 수집 계약: `[T-60, T+121)`이므로 `T-60`부터 `T+120`까지 **181개 후보 timestamp**다. 공급자가 181개 봉을 모두 반환해야 수집 성공인 것은 아니다.
- 서빙 차트: `[T-60, T+120)`의 **180분 표시 범위**다. 현재 CPI·NVDA 화면은 1분봉 180개를 읽는다.

## 발표 직전 1분 체크

1. `docker compose up -d --wait postgres`로 기존 데이터가 있는 DB를 확인한다.
2. API를 시작하고 `curl -fsS http://127.0.0.1:8000/health`에서 `database=ok`를 확인한다.
3. 대시보드에서 CPI 2026-07·NVDA를 미리 선택한다.
4. 아래 시연 명령을 한 번 실행하고 영향 4·전략 1·중복 0·`NO_TRADE`를 확인한다.
5. 실패하면 재수집·장애 재현을 하지 말고 저장된 캡처와 JSON을 보여주며 사전 증거라고 밝힌다.

```bash
.venv/bin/python -m scripts.run_serving_demo \
  --event-id 'CPI|2026-07|2026-08-12T12:30:00Z' \
  --symbol NVDA \
  --output /tmp/serving-demo-rehearsal.json
```

새 DB에는 사전 수집한 시장 데이터가 없으므로 이 명령 하나만으로 같은 결과가 생기지 않는다. 시연은 외부 API·Kafka 대용량 재생·증권사 주문을 호출하지 않는다.

## 발표에서 하지 않을 주장

- 특정 부하 실험의 체결 수를 전체 프로젝트 체결 총량이라고 말하지 않는다.
- 봉이 없는 분을 무거래나 수집 실패로 단정하지 않는다.
- odd lot을 개인투자자 거래와 동일시하지 않는다.
- 대시보드 `LONG`을 현재 자동매수 신호라고 말하지 않는다.
- 모의주문 접수·취소를 실제 체결·수익성·실전 안전성 검증이라고 말하지 않는다.

## 제출 전 검증 결과

- 문서·서빙 targeted tests: 21개 통과
- 전체 suite: 186개 통과, 8개 skip, 17개 subtest 통과
- PostgreSQL market-bar integration: 3개 통과
- paper execution·recovery integration: 2개 통과
- Python compileall 및 Git whitespace 검사: 통과
- Archify 브라우저 검증: showcase 9/9, composition 오류·경고 0, 지정 viewport overflow 0

Paper integration은 기존 코드의 회귀 검증일 뿐 이번 제출에서 전략과 주문을 연결했다는 뜻은 아니다.

## 디스코드 제출 문구

```text
[7차시] 미국 경제 발표·시장 반응 파이프라인

저장소: https://github.com/hjh6709/us-market-intelligence-pipeline
발표 자료: https://github.com/hjh6709/us-market-intelligence-pipeline/blob/main/docs/serving-layer-assignment.md

PostgreSQL 저장 결과를 FastAPI와 시장 이벤트 분석 대시보드에서 실제 조회하도록 연결했습니다.
입력 1조합 → 영향 4행 → 전략 1행 Upsert → 1m/3m/5m·영향 재조회까지 한 명령 0.43초로 검증했고 중복은 0입니다.
현재 운영 상태는 RESEARCH_ONLY / NO_TRADE이며, 대용량 실행·외부 API·장애 재현은 저장된 증거로 대체합니다.
```
