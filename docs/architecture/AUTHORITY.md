# Architecture Authority Index

This page routes questions to their owner; it does not redefine any contract.
When documents disagree, use the authority for the specific concern below.

| Concern | Status | Authority |
| --- | --- | --- |
| Current executable behavior | `CURRENT_IMPLEMENTATION` | code + migrations + tests at the deployed commit |
| CPI W1 Data & Governance target | `TARGET_CANONICAL` | [2026-09-19 CPI W1 design](../superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md) |
| Legacy migration-009 event model | `HISTORICAL_SUPERSEDED` | [data-contracts.md](data-contracts.md) |
| Current FastAPI API | `CURRENT_IMPLEMENTATION` | [api-contracts.md](../engineering/api-contracts.md) |
| Product API target | `NOT_YET_SPECIFIED` | separate future Product Serving specification |
| Runtime/security target | `NOT_YET_SPECIFIED` | separate future Runtime/Security/Readiness specification |
| Current/target progress | `STATUS_LEDGER` | [current-vs-target.md](../engineering/current-vs-target.md) |

## Reading rules

- `CURRENT_IMPLEMENTATION` describes behavior only for the executable commit being
  inspected. A dated snapshot cannot override newer code, migrations, or tests.
- `TARGET_CANONICAL` is a design constraint, not evidence that the target is
  implemented.
- `HISTORICAL_SUPERSEDED` preserves rationale and compatibility context but cannot
  define new CPI W1 semantics.
- `STATUS_LEDGER` records progress and gaps; it is not semantic authority.
- Dated evidence proves only the captured run and never promotes a target to
  current implementation.
