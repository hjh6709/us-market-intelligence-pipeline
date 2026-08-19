# Alpaca Live Market Data Smoke Test

실행일: 2026-08-19

목적: 무료 Alpaca 계정으로 IEX WebSocket 인증·구독·실제 trade 수신이 가능한지 확인하고, 수신 payload가 프로젝트의 raw trade 계약과 맞는지 검증한다.

## 환경

- Python 3.14.6
- `websockets` 15.0.1
- 인증정보: 로컬 `.env`, Git 제외 확인
- 테스트 종목: `SPY`, `QQQ`, `NVDA`

## 결과

| 단계 | 결과 | 증거 |
| --- | --- | --- |
| Test stream 연결 | 성공 | `connected` |
| Test stream 인증 | 성공 | `authenticated` |
| `FAKEPACA` 구독 | 성공 | trade 3건 수신 |
| IEX 연결 | 성공 | `connected` |
| IEX 인증 | 성공 | `authenticated` |
| `SPY`, `QQQ`, `NVDA` 구독 | 성공 | subscription acknowledgement |
| 실제 IEX trade | 성공 | 총 10건 수신 |

실제 수신 구간은 `2026-08-19T13:29:55Z`부터 미국 정규장 시작 직후인 `13:30:00Z`까지다. 수신 건수는 `QQQ` 2건, `NVDA` 4건, `SPY` 4건이다.

## Raw contract 확인

10건 모두 다음 필드를 포함했다.

```text
T  message type
S  symbol
i  provider trade ID
x  exchange
p  price
s  size
c  trade conditions
t  event timestamp
z  tape
```

관찰된 값:

- `x="V"`: IEX exchange code
- `z`: `B` 또는 `C`
- `c`: `@`, `T`, `I`, 공백 condition 등 배열 형태
- `t`: UTC이며 소수점 이하 나노초 정밀도
- `i`: 종목별로 자릿수가 다르므로 고정 크기 정수로 가정하지 않는다.

현재 [데이터 모델](../data-model.md)의 Alpaca raw payload 필드와 일치한다. Collector는 원본 payload를 보존하고 Spark가 type validation과 trade-condition 정책을 적용하는 기존 경계를 유지한다.

## 추가로 확인된 운영 제약

같은 계정으로 IEX WebSocket을 동시에 두 개 열었을 때 `406 connection limit exceeded`가 반환됐다. 따라서 collector 하나가 IEX 연결과 전체 종목 구독을 소유해야 한다는 기존 설계가 실제 계정에서도 확인됐다. Smoke test는 인증 오류가 발생하면 구독을 계속하지 않고 즉시 실패하도록 보완했다.

Test stream의 `FAKEPACA`는 다른 timestamp에도 trade ID `1`을 반복했다. 이에 따라 event ID 계약은 provider trade ID의 전역 유일성을 가정하지 않고 source·feed·symbol·trade ID·event timestamp를 함께 hash하도록 수정했다.

## 재실행 명령

키 값은 출력하거나 명령 인자로 전달하지 않는다.

```bash
.venv/bin/python -m src.live_market_smoke \
  --feed test --symbols FAKEPACA --max-trades 3 --timeout 20

.venv/bin/python -m src.live_market_smoke \
  --feed iex --symbols SPY QQQ NVDA --max-trades 10 --timeout 60
```
