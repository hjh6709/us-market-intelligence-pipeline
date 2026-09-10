# Final 10-minute presentation storyboard

형식 중립 handoff입니다. 다음 턴에서 editable PPTX의 품질이 유지되면 PPTX+matching PDF를 만들고, PDF 렌더가 현저히 안정적이면 presentation-quality PDF를 우선해도 됩니다. 두 형식을 만들 경우 주장·순서·수치는 동일해야 합니다.

## 공통 디자인 계약

- 16:9, main 10장, 약 9분 20초 발표 + 40초 여유
- 제목 32pt 이상, 본문 18pt 이상을 목표로 projector에서 읽히게 구성
- 한 장에 한 결론, action-title headline 사용
- navy/white 기반에 Research=blue, Validation=violet, Product=amber, verified=green, limitation=gray
- 실제 screenshot/evidence를 5장 이상 사용하되 터미널·코드 벽은 피함
- `CURRENT`, `FOUNDATION`, `TARGET`, `HISTORICAL EXPERIMENT` label을 색과 문구로 중복 표시
- 수익성을 암시하는 green P&L 표현 금지; 음수 baseline은 중립 색으로 표시

## Main slides

| # | 시간 | Action title / conclusion | 핵심 내용 | Visual / source | 말할 때 주의 |
| ---: | ---: | --- | --- | --- | --- |
| 1 | 0:50 | **요약을 믿는 대신, 시장 반응을 직접 검증하고 싶었습니다** | 개인 투자자로서 발표 의미·가격 반응·사후 설명을 구분하기 어려웠던 문제 | 실제 Research 화면을 낮은 opacity 배경 + 한 문장 | 특정 서비스 비난 금지 |
| 2 | 0:50 | **경제 캘린더의 숫자만으로는 ‘실제 반응’을 설명할 수 없습니다** | actual/consensus/previous 이후 필요한 질문: 반응, 지속, 이례성, 유사 환경, 가설 검증 | event→questions 흐름 | 현재 consensus adapter가 운영 중인 것처럼 말하지 않음 |
| 3 | 1:10 | **Research·Validation·Product를 분리해 데이터 의미를 지켰습니다** | 세 plane, provider bars와 Raw SIP validation 물리적 분리 | `CURRENT_ARCHITECTURE.mmd` SVG | 202×10이 Kafka/Spark를 통과했다는 인상 금지 |
| 4 | 0:55 | **202회×10종목의 실제 발표 구간을 연구 데이터로 만들었습니다** | 202, 10, 2,020, 308,512, 112,593, 70,090, 2,020 contexts, 8,080 impacts | scale chart + corrective overview crop | 파생·선택 합계를 단일 원본 총량으로 합산 금지 |
| 5 | 1:00 | **GCP에서 736만 Raw SIP를 별도 검증해 부하 경계를 확인했습니다** | GCP e2-standard-4, 7,360,804, Kafka/Spark parity, 22,260 bars, 1,690.25s | load scope crop + simple flow | production deployment/throughput 비교 금지 |
| 6 | 1:05 | **49건의 ‘중복’은 데이터가 아니라 식별자 설계의 오류였습니다** | observed 49→exchange omission→identity correction→7.36M rerun→0 | 새 four-step incident visual | 실제 중복을 제거했다고 말하지 않음 |
| 7 | 0:55 | **DB 장애는 실패로 드러났고, 같은 입력의 복구 재처리로 무결성을 확인했습니다** | DB off OperationalError, restore, 118,118 replay, 472 bars, hash same, duplicate 0 | `02-failure-and-recovery.png` crop | end-to-end exactly-once 주장 금지 |
| 8 | 1:10 | **단순 T±N 분석의 한계를 막기 위한 canonical foundation을 먼저 세웠습니다** | PIT provider-local consensus, append-only revisions, sessions/S0, Reaction-v2, quality taxonomy; 51 counterexamples | target diagram crop + foundation ribbon | foundation=operational feature 아님을 크게 표기 |
| 9 | 1:10 | **현재 제품은 결과를 읽고 수동 Paper 경계까지 확인할 수 있습니다** | Overview/Research/Pipelines, FastAPI; Paper accepted→canceled fill0; Current/Foundation/Target strip | 2–3 current screenshots | automated research order·fill·position proof 아님 |
| 10 | 0:55 | **좋은 플랫폼은 수익을 꾸미지 않고 약한 가설을 기각합니다** | legacy result -0.15649%, positive 39.336%; 다음: observation/consensus adapters→v2→comparables→simulator→Paper experiment | 음수 result + target sequence | 기대수익률·인과관계로 표현 금지 |

예상 main talk: 약 10분. 리허설에서 9분 20초를 목표로 문장을 줄여 질문 전 여유를 둡니다.

## Backup slides

| # | Title | Content | Evidence |
| ---: | --- | --- | --- |
| B1 | 숫자와 실행일 정본 | 모든 scale/runtime/quality 수치와 날짜 | [claims ledger](PRESENTATION_CLAIMS.md) |
| B2 | Airflow는 202 mapped tasks입니다 | DAG chain, pools, retries, 2,020 DB work items 구분 | [Airflow audit](../../engineering/airflow-audit.md) |
| B3 | Current / Foundation / Target 상세 | capability matrix 전체 | [current vs target](../../engineering/current-vs-target.md) |
| B4 | Paper boundary와 미검증 항목 | manual guard, accepted→canceled, fill0, no position proof | [paper evidence](../../paper-execution.md) |

## Slide production checklist

1. 모든 숫자는 [PRESENTATION_CLAIMS.md](PRESENTATION_CLAIMS.md)에서 복사합니다.
2. 모든 screenshot은 [PRESENTATION_ASSET_MAP.md](PRESENTATION_ASSET_MAP.md)의 decision/crop을 따릅니다.
3. slide 3 current 구성도는 `.mmd`에서 SVG로 재현합니다.
4. slide 8 target diagram에는 `APPROVED TARGET — NOT CURRENT IMPLEMENTATION` 문구를 고정합니다.
5. final PPTX/PDF의 factual text를 서로 diff하고 같은 slide count/structure인지 검사합니다.
6. PDF 전체 페이지와 PPTX 렌더를 image montage로 검토해 clipping, overflow, 너무 작은 글자, screenshot 왜곡을 확인합니다.
7. speaker notes에는 각 claim의 evidence path를 넣되 화면 본문에는 필요한 출처만 짧게 표시합니다.
