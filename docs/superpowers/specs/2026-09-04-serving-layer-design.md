# 7차시 서빙 레이어 및 자동매매 확장 설계

## 1. 목표

이번 구현의 목표는 새 전략을 추가하는 것이 아니라, 이미 PostgreSQL에 저장한 경제 발표·시장 반응·탐색 전략 결과를 실제 사용자가 조회하는 장면까지 연결하는 것이다.

최종 프로젝트 목표는 실전 자동매매이지만, 현재 탐색 전략은 과거 평균 순수익률이 음수이고 발표별 전망치·실제값 차이도 아직 없으므로 이번 단계에서 주문을 전송하지 않는다. 이번 화면은 단순히 `NO_TRADE`만 보여주는 조회 화면이 아니라, 실제 데이터 결과·과거 전략 시뮬레이션·자동매매 준비 상태를 분리해 보여준다. 서빙 결과는 향후 주문 엔진이 사용할 수 있는 동일한 판단 계약을 제공하되, 현재 운영 단계는 `RESEARCH_ONLY`, 주문 행동은 `NO_TRADE`로 제한한다.

## 2. 사용자에게 보여줄 결과

브라우저에서 경제 발표 종류, 발표일, 종목을 선택하면 다음 정보를 한 화면에서 확인한다.

- 경제 발표의 종류, 기준 기간, 발표 시각과 데이터 품질
- 선택 종목의 발표 전 60분과 발표 후 5분·30분·60분 수익률
- 각 구간의 거래량, 변동성, 시장 대비 수익률과 커버리지 상태
- 발표 시점에 이용 가능했던 FRED·ALFRED 경제 환경
- 탐색 전략의 신호와 커버리지
- 전체 탐색 전략의 성과 요약
- 과거 가상 진입·청산 가격과 거래비용 차감 결과
- 데이터·전략·주문·복구 준비 상태별 통과 여부
- 현재 운영 단계, 주문 행동과 사람이 이해할 수 있는 차단 사유

1분·3분·5분봉은 서로 다른 원본처럼 합산하지 않는다. 차트는 PostgreSQL의 `market_bars`를 시간봉별로 조회하며, 파생 봉의 `COMPLETE`·`PARTIAL` 상태를 함께 보여준다.

## 3. 사용자 인터페이스

FastAPI 애플리케이션이 JSON API와 한 장짜리 HTML 대시보드를 함께 제공한다. 별도의 프런트엔드 빌드 시스템은 추가하지 않는다.

대시보드는 다음 영역으로 구성한다.

1. 발표 종류·발표일·종목 선택기
2. 발표 정보와 데이터 상태 요약 카드
3. 발표 전후 수익률·거래량·변동성 표
4. 1분·3분·5분 가격 차트
5. 당시 경제 환경 표
6. 탐색 전략의 과거 시뮬레이션 카드
7. 자동매매 준비 상태와 주문 행동 카드

화면은 투자 추천 서비스처럼 수익을 약속하지 않는다. `연구 신호`, `과거 시뮬레이션`, `실제 주문 행동`을 시각적으로 분리하고, 현재 운영 단계와 행동이 `RESEARCH_ONLY`·`NO_TRADE`인 이유를 항상 표시한다.

## 4. API 계약

다음 읽기 전용 엔드포인트를 제공한다.

- `GET /health`: 애플리케이션과 PostgreSQL 연결 상태
- `GET /api/v1/events`: 발표 종류·날짜 범위로 조회할 경제 발표 목록
- `GET /api/v1/events/{event_id}/symbols`: 선택 발표에 저장된 종목 목록
- `GET /api/v1/events/{event_id}/symbols/{symbol}`: 발표 정보, 시장 반응, 경제 환경, 전략 결과와 안전 판단을 합친 상세 결과
- `GET /api/v1/events/{event_id}/symbols/{symbol}/bars?timeframe=1m`: 선택 봉의 차트 데이터
- `GET /api/v1/strategy/summary`: 현재 전략 버전의 전체 성과 요약
- `GET /`: 위 API를 사용하는 발표용 HTML 대시보드

상세 결과의 판단 부분은 다음 의미를 갖는다.

```json
{
  "research_signal": "LONG",
  "simulation": {
    "entry_price": "123.450000",
    "exit_price": "122.800000",
    "gross_return_pct": "-0.526529",
    "transaction_cost_bps": "10.0000",
    "net_return_pct": "-0.626529"
  },
  "execution_readiness": {
    "stage": "RESEARCH_ONLY",
    "order_action": "NO_TRADE",
    "eligible_for_order": false,
    "requires_human_approval": false,
    "checks": [
      {"name": "market_data", "status": "PASS"},
      {"name": "strategy_result", "status": "PASS"},
      {"name": "strategy_performance", "status": "FAIL"},
      {"name": "event_surprise", "status": "FAIL"},
      {"name": "paper_execution", "status": "FAIL"},
      {"name": "position_recovery", "status": "FAIL"},
      {"name": "kill_switch", "status": "FAIL"}
    ],
    "reasons": [
      "exploratory strategy has not passed the performance gate",
      "consensus-versus-actual surprise is unavailable",
      "broker execution and position recovery are not implemented"
    ]
  }
}
```

`research_signal`은 과거 실험에서 계산된 방향일 뿐 주문이 아니다. `simulation`도 해당 과거 구간의 연구 결과다. 실제 행동은 `execution_readiness.order_action`만 나타낸다.

## 5. 데이터 접근 구조

서빙 계층은 데이터베이스 SQL을 HTTP 라우터 안에 직접 흩어 놓지 않는다.

```text
FastAPI route / HTML dashboard / demo command
                    ↓
              ServingService
                    ↓
             PostgresRepository
                    ↓
 PostgreSQL 최종 테이블과 실행 추적 테이블
```

`PostgresRepository`는 기존 테이블을 읽는다.

- `economic_events`: 발표 메타데이터
- `market_bars`: 1분·3분·5분 가격 데이터
- `macro_event_impacts`: 발표 전후 구간별 반응
- `macro_event_contexts` 및 관련 관측 테이블: 당시 경제 환경
- `event_strategy_results`: 탐색 전략 결과
- `pipeline_runs`, `pipeline_work_items`, `pipeline_run_checks`: 실행과 품질 상태

`ServingService`는 숫자를 직렬화 가능한 응답으로 조합하고 과거 시뮬레이션과 자동매매 준비 상태를 계산한다. API와 발표용 실행 명령은 같은 서비스 함수를 사용해 서로 다른 결과가 나오지 않게 한다.

## 6. 안전 판단 규칙

이번 버전의 판단은 수익을 예측하는 새 전략이 아니라 현재 자동매매 준비 상태를 점검하는 위험 게이트다. 준비 상태는 각 검사를 `PASS` 또는 `FAIL`로 보여주며, 전체 운영 단계를 별도로 표시한다.

다음 조건을 모두 만족하지 못하면 `NO_TRADE`를 반환한다.

- 선택한 발표·종목의 필수 시장 반응 데이터가 계산 가능할 것
- 전략 결과의 커버리지가 주문 판단에 사용 가능한 상태일 것
- 현재 전략 버전이 별도로 정한 성과 승인을 통과할 것
- 해당 발표의 전망치·실제값과 surprise가 준비될 것
- 주문, 체결 추적, 포지션 복구와 긴급 중지 기능이 구현·활성화될 것

현재 저장 상태에서는 마지막 세 조건을 만족하지 않으므로 운영 단계는 `RESEARCH_ONLY`, 주문 행동은 `NO_TRADE`다. 이는 하드코딩된 문구가 아니라 준비 상태를 명시적으로 검사한 결과로 표현한다.

운영 단계는 향후 `RESEARCH_ONLY` → `PAPER_TRADING` → `HUMAN_APPROVAL` → `LIMITED_LIVE` → `AUTOMATED_LIVE` 순서로만 승격한다. 이번 구현은 다음 단계의 이름과 응답 계약만 정의하며 승격 기능은 제공하지 않는다.

LLM은 주문 방향이나 수량을 결정하지 않는다. 설명 문구도 데이터베이스에 저장된 수치와 결정 규칙에서 생성한다.

## 7. 한 번에 이어진 실행 기록

발표에서는 외부 API와 대용량 Kafka 재생을 실행하지 않는다. 대신 이미 저장된 실제 데이터를 사용하는 작은 재현 명령 하나를 제공한다.

입력은 `event_id`, `symbol`과 데이터베이스 연결 정보다. 명령은 다음 순서로 실행한다.

1. 선택한 발표와 1분봉이 PostgreSQL에 존재하는지 확인
2. 기존 분석 함수로 해당 범위의 발표 반응을 다시 계산해 Upsert
3. 기존 전략 함수로 탐색 전략 결과를 다시 계산해 Upsert
4. `ServingService`로 방금 저장한 결과를 다시 읽음
5. 입력·처리·저장·읽기 건수와 최종 판단을 JSON으로 출력

재실행은 기존 고유키와 Upsert를 사용하므로 중복 행을 만들지 않는다. 외부 API 호출이나 실주문은 포함하지 않아 1~2분 발표 범위에서 안전하게 반복할 수 있다.

## 8. 검증과 증거

다음 검증을 자동화한다.

- 저장소 계층의 SQL 결과 매핑 테스트
- 존재하지 않는 발표·종목·봉에 대한 404 및 입력 검증 테스트
- 전략 신호·과거 시뮬레이션·주문 판단이 분리되는지 확인하는 안전 규칙 테스트
- 준비 상태의 각 검사가 `PASS`·`FAIL`로 노출되는지 확인하는 테스트
- 현재 조건에서 주문 가능 결과가 절대 나오지 않는지 확인하는 테스트
- API 응답 스키마와 대시보드 기본 렌더링 테스트
- 동일 입력을 두 번 실행했을 때 최종 행 수가 증가하지 않는 통합 테스트
- 작은 실제 데이터 실행의 입력·처리·저장·읽기 건수 기록

공개 가능한 실행 JSON과 대시보드 캡처는 `docs/evidence/serving-layer/`에 저장한다. API 키, 데이터베이스 비밀번호와 원시 대용량 데이터는 포함하지 않는다.

## 9. 문서와 발표

다음 문서를 현재 구현과 일치하게 갱신한다.

- 루트 `README.md`: 실행 방법, 최신 구성도, 서빙 결과 확인 방법
- `docs/serving-layer-assignment.md`: 과제 요구사항별 구현·실행 증거
- `docs/09.07_대본.md`: 약 4분 발표 대본과 1~2분 시연 순서
- 최신 아키텍처 그림: 현재 구현과 향후 자동매매 계층을 선 종류 또는 영역으로 구분

발표에서는 다음을 명확히 구분한다.

- 실제 구현: 저장 결과 조회, 상세 API, 대시보드, 과거 시뮬레이션, 자동매매 준비 상태, 한 번의 재현 실행
- 과거 실행 증거: Kafka·Spark 부하, 장애·복구, Airflow 전체 실행
- 다음 단계: Slack 승인, Alpaca 모의주문, 주문·부분 체결·포지션 복구, 실전 자동매매

## 10. 이번 구현에서 제외하는 범위

다음 항목은 최종 목표에는 포함되지만 7차시 구현에는 포함하지 않는다.

- 실계좌 주문과 자동 청산
- Alpaca·키움 주문 API 연결
- Slack 승인 버튼
- 부분 체결·미체결 정정·취소와 계좌 복구
- 뉴스 LLM, VCP, RSI, MACD 등 새 전략
- 전망치 데이터 공급자 신규 계약 또는 비공식 스크래핑
- Java/Spring 기반 신규 서비스

이 범위를 제외하는 이유는 자동매매를 포기해서가 아니라, 저장 결과를 사용하는 서빙 계층을 먼저 완성하고 주문 계층이 신뢰할 수 있는 계약을 만드는 것이 이번 과제의 목적이기 때문이다.

## 11. 최종 자동매매 확장 경로

서빙 계층 이후에는 동일한 판단 계약을 확장한다.

```text
RESEARCH_ONLY / NO_TRADE
  ↓ 전략·데이터 승인
PAPER_TRADING
  ↓ 모의주문·체결·복구 검증
HUMAN_APPROVAL
  ↓ Slack 사람 승인과 소액 제한
LIMITED_LIVE
  ↓ 운영 안정성 검증
AUTOMATED_LIVE
```

주문 엔진은 전략 엔진과 분리하고 증권사별 어댑터를 둔다. 실전 전환 전에는 최대 주문 금액, 최대 보유 종목, 일일 손실 한도, 중복 주문 방지, 거래 시간 확인, 긴급 중지, 주문 접수·부분 체결·완전 체결 상태, 재시작 후 계좌 대사를 검증한다.
