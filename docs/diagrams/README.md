# 아키텍처 다이어그램

이 디렉터리는 README와 발표에서 사용하는 데이터 파이프라인 다이어그램을 관리한다.

| 파일 | 용도 |
| --- | --- |
| `target-platform.architecture.json` | `63633a5` source evidence에 고정한 approved target 정본; 현재 구현 완료를 뜻하지 않음 |
| `target-platform.html` | current/evolving/foundation tag와 plane 경계를 탐색하는 target 구성도 |
| `target-platform.visual-check.*` | target 구성도의 4개 viewport·양 테마 자동 검증 receipt와 캡처 |
| `target-platform.manual-review.json` | delivered artifact hash에 고정한 light/dark 시각 검토 기록 |
| `session7-architecture.architecture.json` | merged main `7f55721` 소스 증거를 연결한 Archify 정본 |
| `session7-architecture.html` | 검색·guided view·trace motion이 있는 제출용 대화형 구성도 |
| `session7-architecture.visual-check.*` | 4개 데스크톱 viewport와 라이트·다크 자동 브라우저 검증 receipt·캡처 |
| `session7-architecture.manual-review.json` | 전달된 artifact에 결합한 별도 수동 시각 검토 기록 |
| `pipeline-architecture.svg` | 전체 프로젝트 정본. CPI raw 부하, 202회·10종목 시장·경제 데이터, 이벤트 분석, 실제 서빙과 미구현 주문 계층을 구분 |
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
- 보라색 점선: 아직 실행하지 않은 전망치·surprise, 모의주문, 위험관리와 실전 주문 계층
- 주황색: 실제 실행 결과와 건수·coverage 검증 증거
- 루트 README에는 전체 프로젝트 그림만 배치하고, 과제 실행 그림은 과제 문서에서만 사용한다.

PNG를 수정한 뒤에는 SVG 정본과 내용이 같은지, 글자가 잘리거나 lane 경계를 침범하지 않는지 확인한다.

## Archify 제출 구성도

7차시 정본은 [Archify](https://github.com/tt-a1i/archify) 2.17로 생성했다.
showcase validation 9/9, composition 오류·경고 0이며 자동 브라우저 검사는
1440×900, 1600×1000, 1920×1080, 2048×1320에서 overflow 없음과 라이트·다크 캡처를 확인했다.
자동 receipt의 `visualReview: pending`은 브라우저 자동 검사가 사람의 시각 판단을 대신하지 않는다는 뜻이다.
실제 캡처 검토 결과는 별도의 `session7-architecture.manual-review.json`에 artifact hash와 함께 기록한다.
HTML의 고정 Viewer UI는 영어 fallback이고, 작성한 노드·설명은 한국어다.

2026-09-10 target 구성도도 showcase validation 9/9, 오류·경고 0을 통과했다. Chrome 자동 검사는 1440×900, 1600×1000, 1920×1080, 2048×1320에서 overflow 없음, 최소 글자 6px, 라이트·다크 캡처를 확인했다. 이 구성도는 approved destination이며 `CURRENT`, `EVOLVING`, `P1 FOUNDATION` tag를 구현 증거처럼 읽으면 안 된다.

## PNG 생성

macOS의 Chrome headless 모드로 SVG 정본을 PNG로 렌더링한다. SVG와 같은
`1600 × 1290` 크기를 지정해야 오른쪽 끝이 잘리지 않는다.

```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless=new --disable-gpu --hide-scrollbars \
  --force-device-scale-factor=1 --window-size=1600,1290 \
  --screenshot="$PWD/docs/diagrams/pipeline-architecture.png" \
  "file://$PWD/docs/diagrams/pipeline-architecture.svg"
```
