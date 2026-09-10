# Repository visibility audit — 2026-09-10

## Exact baseline

- Repository: `/Users/hanjeonghyun/dev/2026/for_my_stock`
- Audit branch: `presentation/final-repository-audit-2026-09-10`
- Baseline and `origin/main`: `56334a89ceff48f63e93b5c260df28e16179b74e`
- PR #36: merged as that commit; source head `9443da525e2f12e38ded24b150feb81a7183c54a`
- Merge-commit push CI: [success](https://github.com/hjh6709/us-market-intelligence-pipeline/actions/runs/34478926754)
- Main checkout was clean before this audit and fast-forwarded from local `316a8a3` to `origin/main`.

## Worktree recovery inventory

모든 worktree의 tracked/untracked status는 clean이었습니다. presentation-relevant ignored files는 실제 main checkout에만 있었습니다.

| Worktree / branch | HEAD | Main relation | Class | Finding |
| --- | --- | --- | --- | --- |
| root / audit branch | `56334a8` | exact main baseline | A | 이 audit package 작성 위치 |
| assignment-ingestion-evidence | `48414e3` | main ancestor | A/F | 내용은 이미 main history에 포함 |
| corrective-engineering | `eabb518` | main ancestor | A/F | 이미 main에 포함, newer PR36가 supersede |
| final-portfolio-completion | `6ebfc9d` | main ancestor | A/F | 이미 main에 포함 |
| fred-airflow-pipeline | `ce81fa2` | 9 commits ahead, 158 behind | B/H/F | remote branch에만 존재; current main의 newer macro path와 충돌 가능, 회수 안 함 |
| market-context-coverage | `a03686d` | main ancestor | A/F | 이미 main에 포함 |
| paper-execution | `dfbcf44` | main ancestor | A/F | 이미 main에 포함 |
| paper-position-recovery / docs-session7-final-audit | `8151642` | main ancestor | A/F | 이미 main에 포함 |
| platform-contract-2026-09-10 | `9443da5` | main parent | A | PR #36 source, main merge 포함 |
| serving-layer | `1d4d830` | main ancestor | A/F | 이미 main에 포함 |
| session7-evidence-final | `521282c` | main ancestor | A/F | 이미 main에 포함 |

`fred-airflow-pipeline`의 9개 unique commit은 `origin/fred-airflow-pipeline`에도 존재하므로 “어떤 remote에도 없는 commit”은 없습니다. 이 branch는 오래된 별도 설계이므로 발표 준비 branch에 cherry-pick하지 않습니다.

## Ignored/local-only inventory

| Item | Class | Decision |
| --- | --- | --- |
| `.env*` | G | secrets; 계속 ignore |
| `data/archive/` Raw SIP Parquet | G | licensed/large raw data; 절대 commit하지 않음 |
| `data/local/`, `*.db`, `*.sqlite*`, dump/backup | G | runtime/local state; 계속 ignore |
| `.venv/`, cache, Airflow runtime, logs, `.tmp/` | F/G | 재생성 가능 runtime; 계속 ignore |
| `docs/evidence/presentation-captures/captures/presentation-script.local.md` | E/F | 오래된 3차시 local script, 현재 사실과 충돌; 의도적으로 미회수 |
| `docs/presentation/us-market-pipeline-assignment.pptx.inspect.ndjson` | E/F | 검사 임시 산출물; final source 아님 |
| `.tmp/.../us-market-pipeline-assignment.pptx` | E/F | tracked 과거 PPT와 SHA-256 동일; 중복 회수 불필요 |
| `docs/.DS_Store` | F | OS metadata; 계속 ignore |

## `.gitignore` verdict

현재 규칙은 secrets, raw/licensed data, runtime, caches와 임시 render를 적절히 막습니다. PNG/JPG/SVG/JSON/HTML/MD/PPTX/PDF를 전역 ignore하지 않으므로 안전한 발표 증거는 Git에 넣을 수 있습니다. 넓은 규칙을 제거할 필요가 없어 `.gitignore`는 변경하지 않았습니다.

주의할 규칙은 `docs/evidence/**/captures/`입니다. 초벌 캡처는 local-only로 두는 의도에 맞지만, 최종 검토된 screenshot은 `captures/` 밖의 명시적인 evidence 경로에 저장해야 합니다.

## Missing/unpublished artifacts found

- 현재 truth를 정확히 표현하는 slide-neutral current architecture source가 없었습니다. 이 PR에서 `CURRENT_ARCHITECTURE.mmd`를 추가했습니다.
- final deck용 claim ledger, asset decision log, worktree visibility audit와 10분 storyboard가 없었습니다. 이 package로 추가했습니다.
- ignored local script는 stale이라 미회수했습니다.
- 새 screenshot이나 새로운 실행 evidence가 worktree-only 상태로 남아 있지는 않았습니다.

## Repository truth corrections

1. 최신 daily selected count는 30,270입니다. `docs/load-recovery-assignment.md`, archived 09.03 script와 `pipeline-architecture`의 30,250은 과거/stale입니다.
2. 기존 Session 7 Archify diagram은 당시 제출 자산입니다. PR36 이후 Raw SIP validation sink isolation의 current 정본으로 사용하지 않습니다.
3. 과거 `market_bars`로 쓰던 Raw replay 캡처는 migration 009 이후 current validation storage를 설명하지 못합니다.
4. `COMPLETE 1,980 / DATA_NOT_AVAILABLE 40`은 coverage correction 전 historical run label입니다. 최신 request completion 및 observed quality와 섞지 않습니다.
5. 과거 Python 3.14 CI mismatch 설명과 45-test screenshot은 PR36의 current verification을 설명하지 못합니다.
6. `Macro Pulse` 명칭이 있는 dashboard screenshot은 obsolete입니다. 정본 제품명은 **Economic Event Intelligence & Strategy Validation Platform**입니다.

## Recovered artifacts

다른 worktree에서 복사할 안전한 최신 artifact는 없었습니다. 회수 대신 main code/evidence를 근거로 새 presentation-neutral 문서와 current architecture source를 작성했습니다. raw data, secrets, stale script, duplicate temp PPT는 이동하지 않았습니다.

## Remaining hygiene recommendations

- 과거 과제 문서·이미지는 삭제하지 말고 향후 `HISTORICAL` banner를 추가할 수 있습니다.
- final deck 생성 시 editable source와 matching PDF/PPTX만 명시적인 final release 폴더에 저장하고 temp renders는 `.tmp/`에 둡니다.
- current browser screen은 deck 제작 직전에 한 번 더 캡처해 commit SHA와 날짜를 metadata에 기록합니다.
