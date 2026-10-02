from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


MIGRATION_DIR = Path("db/migrations")
IMMUTABLE_BASELINE_MANIFEST = MIGRATION_DIR / "immutable-001-009.sha256"
_MANIFEST_LINE = re.compile(
    r"^(?P<digest>[0-9a-f]{64})  (?P<path>db/migrations/00[1-9]_[^/]+\.sql)$"
)


@dataclass(frozen=True)
class MigrationHash:
    digest: str
    path: Path

    def with_digest(self, digest: str) -> "MigrationHash":
        return replace(self, digest=digest)


@dataclass(frozen=True)
class MigrationEquivalenceResult:
    migration_names: tuple[str, ...]
    differences: tuple[str, ...]
    legacy_fixture_preserved: bool


def load_hash_manifest(path: Path) -> tuple[MigrationHash, ...]:
    entries: list[MigrationHash] = []
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        match = _MANIFEST_LINE.fullmatch(raw_line)
        if match is None:
            raise ValueError(f"invalid migration manifest line {line_number}")
        entries.append(
            MigrationHash(
                digest=match.group("digest"),
                path=Path(match.group("path")),
            )
        )
    if len(entries) != 9:
        raise ValueError("immutable migration manifest must contain exactly 001-009")
    return tuple(entries)


def verify_immutable_baseline(
    entries: Iterable[MigrationHash],
) -> tuple[str, ...]:
    failures: list[str] = []
    for entry in entries:
        if not entry.path.is_file():
            failures.append(f"missing migration: {entry.path.as_posix()}")
            continue
        actual = hashlib.sha256(entry.path.read_bytes()).hexdigest()
        if actual != entry.digest:
            failures.append(
                f"migration hash mismatch: {entry.path.as_posix()} "
                f"expected={entry.digest} actual={actual}"
            )
    return tuple(failures)


def migration_paths() -> tuple[Path, ...]:
    return tuple(sorted(MIGRATION_DIR.glob("[0-9][0-9][0-9]_*.sql")))


_SNAPSHOT_QUERIES = {
    "columns": """
        SELECT c.relname, a.attnum, a.attname,
               format_type(a.atttypid, a.atttypmod), a.attnotnull,
               COALESCE(pg_get_expr(d.adbin, d.adrelid), ''),
               a.attidentity, a.attgenerated
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_attribute a ON a.attrelid = c.oid
          LEFT JOIN pg_attrdef d
            ON d.adrelid = a.attrelid AND d.adnum = a.attnum
         WHERE n.nspname = 'public'
           AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
           AND a.attnum > 0 AND NOT a.attisdropped
         ORDER BY c.relname, a.attnum
    """,
    "constraints": """
        SELECT c.relname, con.conname, con.contype,
               con.condeferrable, con.condeferred,
               pg_get_constraintdef(con.oid, false)
          FROM pg_constraint con
          JOIN pg_class c ON c.oid = con.conrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public'
         ORDER BY c.relname, con.conname
    """,
    "indexes": """
        SELECT t.relname, i.relname, pg_get_indexdef(x.indexrelid),
               COALESCE(pg_get_expr(x.indexprs, x.indrelid), ''),
               COALESCE(pg_get_expr(x.indpred, x.indrelid), '')
          FROM pg_index x
          JOIN pg_class t ON t.oid = x.indrelid
          JOIN pg_class i ON i.oid = x.indexrelid
          JOIN pg_namespace n ON n.oid = t.relnamespace
         WHERE n.nspname = 'public'
         ORDER BY t.relname, i.relname
    """,
    "views": """
        SELECT c.relname, pg_get_viewdef(c.oid, false)
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm')
         ORDER BY c.relname
    """,
    "functions": """
        SELECT p.proname, pg_get_function_identity_arguments(p.oid),
               pg_get_function_result(p.oid), p.prokind,
               pg_get_functiondef(p.oid)
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public'
         ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
    """,
    "triggers": """
        SELECT c.relname, t.tgname, pg_get_triggerdef(t.oid, false)
          FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public' AND NOT t.tgisinternal
         ORDER BY c.relname, t.tgname
    """,
    "sequences": """
        SELECT sequencename, data_type, start_value, min_value, max_value,
               increment_by, cycle, cache_size
          FROM pg_sequences
         WHERE schemaname = 'public'
         ORDER BY sequencename
    """,
    "sequence_ownership": """
        SELECT seq.relname, COALESCE(tbl.relname, ''),
               COALESCE(att.attname, '')
          FROM pg_class seq
          JOIN pg_namespace n ON n.oid = seq.relnamespace
          LEFT JOIN pg_depend dep
            ON dep.classid = 'pg_class'::regclass
           AND dep.objid = seq.oid AND dep.deptype = 'a'
          LEFT JOIN pg_class tbl ON tbl.oid = dep.refobjid
          LEFT JOIN pg_attribute att
            ON att.attrelid = dep.refobjid AND att.attnum = dep.refobjsubid
         WHERE n.nspname = 'public' AND seq.relkind = 'S'
         ORDER BY seq.relname
    """,
}

_REFERENCE_TABLES = (
    "data_sources",
    "observation_definitions",
    "economic_observation_registry",
)


def _schema_snapshot(connection: psycopg.Connection) -> dict[str, tuple[tuple, ...]]:
    snapshot: dict[str, tuple[tuple, ...]] = {}
    for name, query in _SNAPSHOT_QUERIES.items():
        snapshot[name] = tuple(connection.execute(query).fetchall())
    for table in _REFERENCE_TABLES:
        exists = connection.execute(
            "SELECT to_regclass(%s)", (f"public.{table}",)
        ).fetchone()[0]
        if exists is None:
            snapshot[f"reference:{table}"] = ()
            continue
        rows = connection.execute(
            sql.SQL("SELECT to_jsonb(t)::text FROM {} t ORDER BY 1").format(
                sql.Identifier(table)
            )
        ).fetchall()
        snapshot[f"reference:{table}"] = tuple(rows)
    return snapshot


def _apply(connection: psycopg.Connection, paths: Iterable[Path]) -> None:
    for path in paths:
        connection.execute(path.read_text(encoding="utf-8"))


def _insert_legacy_fixture(connection: psycopg.Connection) -> None:
    connection.execute(
        """
        INSERT INTO market_bars (
            symbol, bar_start, timeframe, open, high, low, close,
            volume, trade_count, vwap, source, feed, is_final,
            condition_policy, spark_batch_id
        ) VALUES (
            'CPIW1_FIXTURE', '2026-01-01T00:00:00Z', '1m',
            100, 101, 99, 100.5, 10, 2, 100.25,
            'MIGRATION_TEST', 'FIXTURE', TRUE, 'NONE', 1
        )
        """
    )


def _database_dsn(base_dsn: str, dbname: str) -> str:
    params = conninfo_to_dict(base_dsn)
    params["dbname"] = dbname
    return make_conninfo(**params)


def compare_fresh_and_upgrade(base_dsn: str) -> MigrationEquivalenceResult:
    entries = load_hash_manifest(IMMUTABLE_BASELINE_MANIFEST)
    failures = verify_immutable_baseline(entries)
    if failures:
        raise RuntimeError("; ".join(failures))

    paths = migration_paths()
    if tuple(path.name[:3] for path in paths[:9]) != tuple(
        f"{number:03d}" for number in range(1, 10)
    ):
        raise RuntimeError("migration sequence must begin with contiguous 001-009")

    token = uuid4().hex[:12]
    fresh_name = f"cpi_w1_fresh_{token}"
    upgrade_name = f"cpi_w1_upgrade_{token}"
    admin_params = conninfo_to_dict(base_dsn)
    admin_name = admin_params.get("dbname") or "postgres"
    admin_dsn = _database_dsn(base_dsn, admin_name)

    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        for name in (fresh_name, upgrade_name):
            admin.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                    sql.Identifier(name)
                )
            )

    try:
        with psycopg.connect(_database_dsn(base_dsn, fresh_name)) as fresh:
            _apply(fresh, paths)
            fresh_snapshot = _schema_snapshot(fresh)

        with psycopg.connect(_database_dsn(base_dsn, upgrade_name)) as upgrade:
            _apply(upgrade, paths[:9])
            _insert_legacy_fixture(upgrade)
            _apply(upgrade, paths[9:])
            fixture_count = upgrade.execute(
                "SELECT count(*) FROM market_bars WHERE symbol = 'CPIW1_FIXTURE'"
            ).fetchone()[0]
            upgrade_snapshot = _schema_snapshot(upgrade)

        differences = tuple(
            name
            for name in sorted(set(fresh_snapshot) | set(upgrade_snapshot))
            if fresh_snapshot.get(name) != upgrade_snapshot.get(name)
        )
        return MigrationEquivalenceResult(
            migration_names=tuple(path.name for path in paths),
            differences=differences,
            legacy_fixture_preserved=fixture_count == 1,
        )
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            for name in (fresh_name, upgrade_name):
                admin.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(name)
                    )
                )
