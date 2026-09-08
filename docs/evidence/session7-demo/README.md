# 7차시 발표 시연 증거

## 실제 단일 실행 기록 — 2026-09-08

- 원본 터미널 기록: `session7-actual-e2e.typescript`
- 바로 읽는 텍스트 사본: `session7-actual-e2e.txt`
- 실행 명령과 입력·처리·저장·읽기 결과를 timestamp와 함께 기록
- 결과: 영향 4행, 전략 1행 Upsert, 중복 0, 1m·3m·5m 재조회, `RESEARCH_ONLY / NO_TRADE`

다음 명령으로 실제 터미널 세션을 재생할 수 있다.

```bash
script -p docs/evidence/session7-demo/session7-actual-e2e.typescript
```

`.typescript`는 정적 재현 화면이 아니라 macOS `script -r`로 저장한 실제 명령 실행 원본이다. `.txt`는 같은 원본을 `script -dp`로 재생해 만든 읽기용 사본이다.

## 실제 저장 결과 조회 화면 녹화 — 2026-09-08

- 파일: `session7-live-dashboard.mp4`
- 길이: 65초
- 화면: 1920×1246, H.264 MP4, 무음
- 내용: 실행 중인 FastAPI 대시보드에서 CPI 2026-07·NVDA를 선택하고 PostgreSQL에 저장된 1분·3분·5분봉, 이벤트 영향 4행, 경제 환경, 연구 신호와 `RESEARCH_ONLY / NO_TRADE`를 조회

이 영상은 실제 브라우저 화면 녹화다. 입력→처리→저장→읽기 명령의 원본은 위
`.typescript`와 `.txt`가 담당한다. 두 증거를 합쳐 하나의 실제 영상인 것처럼
표현하지 않는다.

## 보조 발표 영상 — 2026-09-07

- 일반 재생용: `session7-submission-demo.mp4`
- 한국어 자막 트랙 포함: `session7-submission-demo-captioned.mp4`
- 자막 원본: `session7-submission-demo.ko.srt`
- 녹화 원본: `session7-submission-demo.webm`
- 길이: 70.32초
- 화면: 1440×900, VP8 WebM, 무음
- 생성일: 2026-09-07

## 녹화 순서

1. Archify 전체 구성도
2. `이벤트 컨텍스트 수집`, `체결 부하·복구`, `분석·서빙` guided view
3. 실제 리허설 출력값을 정리한 발표용 캡처
4. 로컬 `시장 이벤트 분석 대시보드`
5. CPI 2026-07·NVDA 선택과 `저장 결과 조회`
6. 1분봉 차트, 이벤트 반응, 경제 환경, `RESEARCH_ONLY / NO_TRADE`

녹화 직전에 같은 명령을 다시 실행해 출력값을 확인했고, 녹화 중에는 정리한 실행 출력과
로컬 PostgreSQL·FastAPI 저장 결과만 표시했다. 외부 시장·경제 데이터 API와
브로커 주문 API는 호출하지 않았다. 영상은 발표 중 실행 실패 시 사용할 사전 증거이며,
실제 단일 실행의 원본 증거는 위 `session7-actual-e2e.typescript`다.
