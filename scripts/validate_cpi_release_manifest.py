from __future__ import annotations

import json
import sys
from pathlib import Path

from src.cpi_ingestion import load_cpi_releases


def validate(path: Path) -> dict[str, object]:
    releases = load_cpi_releases(path)
    released_dates = [item.release_date.isoformat() for item in releases]
    reference_periods = [item.reference_period for item in releases]
    duplicate_dates = len(released_dates) - len(set(released_dates))
    duplicate_periods = len(reference_periods) - len(set(reference_periods))

    if released_dates != sorted(released_dates):
        raise ValueError("release dates must be chronological")
    if duplicate_dates or duplicate_periods:
        raise ValueError("release dates and reference periods must be unique")

    return {
        "events": len(releases),
        "first": released_dates[0],
        "last": released_dates[-1],
        "duplicate_release_dates": duplicate_dates,
        "duplicate_reference_periods": duplicate_periods,
        "unpublished_reference_periods": ["2025-10"],
    }


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("config/cpi_releases.json")
    print(json.dumps(validate(path), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
