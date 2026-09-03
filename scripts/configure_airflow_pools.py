"""Create or update the bounded Airflow pools used by this repository."""

from __future__ import annotations

import subprocess


POOLS = {
    "alpaca_api_pool": 2,
    "fred_api_pool": 1,
    "spark_pool": 1,
    "postgres_write_pool": 2,
}


def main() -> int:
    for name, slots in POOLS.items():
        subprocess.run(
            [
                "airflow",
                "pools",
                "set",
                name,
                str(slots),
                f"Managed pool for {name.replace('_', ' ')}",
            ],
            check=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
