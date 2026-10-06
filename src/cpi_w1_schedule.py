"""Deterministic CPI schedule evidence parsing.

Parsers return typed candidates only. They do not create canonical DB rows and
never infer cancellation or date-pending state from absence.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

from src.cpi_w1_contracts import (
    ScheduleMaterial,
    ScheduleStatus,
    SourceAuthorityRole,
    TimePrecision,
)


_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_PENDING_TOKENS = {"tba", "to be announced", "date pending", "pending"}
_CANCELED_TOKENS = {"canceled", "cancelled"}

_MAX_SCHEDULE_HTML_BYTES = 5 * 1024 * 1024
_MAX_SCHEDULE_TABLES = 64
_MAX_SCHEDULE_ROWS_PER_TABLE = 5000
_MAX_SCHEDULE_CELLS_PER_ROW = 64
_MAX_SCHEDULE_CELL_TEXT_CHARS = 8192
_MAX_ICS_BYTES = 2 * 1024 * 1024
_MAX_ICS_LINES = 100000
_MAX_ICS_EVENTS = 5000
_MAX_ICS_LINE_CHARS = 65536


class ScheduleParseError(ValueError):
    pass


@dataclass(frozen=True)
class ScheduleCandidate:
    artifact_content_sha256: str
    reference_month: date
    schedule_status: ScheduleStatus
    scheduled_date: date | None
    scheduled_at: datetime | None
    schedule_timezone: str | None
    time_precision: TimePrecision | None
    source_role: SourceAuthorityRole
    extractor_contract_version: str
    source_effective_date: date | None = None
    source_effective_at: datetime | None = None
    source_effective_precision: TimePrecision | None = None

    @property
    def material(self) -> ScheduleMaterial:
        return ScheduleMaterial(
            schedule_status=self.schedule_status,
            scheduled_date=self.scheduled_date,
            scheduled_at=self.scheduled_at,
            schedule_timezone=self.schedule_timezone,
            time_precision=self.time_precision,
        )

    @property
    def material_fingerprint(self) -> str:
        return self.material.fingerprint


class _SemanticTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._table_depth = 0
        self._rows: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                if len(self.tables) >= _MAX_SCHEDULE_TABLES:
                    raise ScheduleParseError("too many schedule tables")
                self._rows = []
        elif self._table_depth == 1 and tag == "tr":
            if self._rows is not None and len(self._rows) >= _MAX_SCHEDULE_ROWS_PER_TABLE:
                raise ScheduleParseError("too many schedule rows")
            self._row = []
        elif self._table_depth == 1 and tag in {"th", "td"}:
            if self._row is not None and len(self._row) >= _MAX_SCHEDULE_CELLS_PER_ROW:
                raise ScheduleParseError("too many schedule cells in row")
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self._table_depth == 1 and tag in {"th", "td"} and self._cell_parts is not None:
            text = " ".join("".join(self._cell_parts).split())
            if len(text) > _MAX_SCHEDULE_CELL_TEXT_CHARS:
                raise ScheduleParseError("schedule table cell exceeds text limit")
            if self._row is not None:
                self._row.append(text)
            self._cell_parts = None
        elif self._table_depth == 1 and tag == "tr":
            if self._rows is not None and self._row:
                self._rows.append(self._row)
            self._row = None
        elif tag == "table" and self._table_depth:
            if self._table_depth == 1 and self._rows is not None:
                self.tables.append(self._rows)
                self._rows = None
            self._table_depth -= 1


def _parse_reference_month(text: str) -> date:
    match = re.fullmatch(r"\s*([A-Za-z]+)\s+(\d{4})\s*", text)
    if not match:
        raise ScheduleParseError(f"invalid CPI reference month: {text!r}")
    month = _MONTHS.get(match.group(1).lower())
    if month is None:
        raise ScheduleParseError(f"unknown CPI reference month: {text!r}")
    return date(int(match.group(2)), month, 1)


def _parse_release_date(text: str) -> date:
    cleaned = " ".join(text.replace(",", " ").split())
    match = re.fullmatch(r"([A-Za-z]{3,9})\.?\s+(\d{1,2})\s+(\d{4})", cleaned)
    if not match:
        raise ScheduleParseError(f"invalid CPI release date: {text!r}")
    month_token = match.group(1).rstrip(".").lower()
    month_matches = [number for name, number in _MONTHS.items() if name.startswith(month_token)]
    if len(set(month_matches)) != 1:
        raise ScheduleParseError(f"ambiguous CPI release month: {text!r}")
    return date(int(match.group(3)), month_matches[0], int(match.group(2)))


def _parse_release_time(text: str, *, release_date: date, timezone_name: str) -> datetime:
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*([AaPp][Mm])\s*", text)
    if not match:
        raise ScheduleParseError(f"invalid CPI release time: {text!r}")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if not 1 <= hour <= 12 or not 0 <= minute <= 59:
        raise ScheduleParseError(f"invalid CPI release time: {text!r}")
    if match.group(3).lower() == "pm" and hour != 12:
        hour += 12
    elif match.group(3).lower() == "am" and hour == 12:
        hour = 0
    return datetime(
        release_date.year,
        release_date.month,
        release_date.day,
        hour,
        minute,
        tzinfo=ZoneInfo(timezone_name),
    )


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z]+", " ", value.lower()).strip()


def parse_cpi_schedule_html(
    body: bytes,
    *,
    expected_reference_month: date,
    extractor_contract_version: str,
    timezone_name: str = "America/New_York",
) -> ScheduleCandidate:
    if len(body) > _MAX_SCHEDULE_HTML_BYTES:
        raise ScheduleParseError("CPI schedule HTML exceeds parser byte limit")
    if expected_reference_month.day != 1:
        raise ValueError("expected_reference_month must be the first day of the month")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ScheduleParseError("CPI schedule HTML must be UTF-8") from exc

    parser = _SemanticTableParser()
    parser.feed(text)

    matching_tables: list[tuple[dict[str, int], list[list[str]]]] = []
    for rows in parser.tables:
        if not rows:
            continue
        normalized = [_normalize_header(cell) for cell in rows[0]]
        required = {"reference month", "release date", "release time"}
        if required.issubset(set(normalized)):
            indexes = {name: normalized.index(name) for name in required}
            matching_tables.append((indexes, rows[1:]))

    if len(matching_tables) != 1:
        raise ScheduleParseError(
            "expected exactly one semantic CPI schedule table with required headers"
        )

    indexes, rows = matching_tables[0]
    candidates: list[ScheduleCandidate] = []
    for row in rows:
        needed_index = max(indexes.values())
        if len(row) <= needed_index:
            continue
        try:
            reference_month = _parse_reference_month(row[indexes["reference month"]])
        except ScheduleParseError:
            continue
        if reference_month != expected_reference_month:
            continue

        release_date_text = row[indexes["release date"]].strip()
        release_time_text = row[indexes["release time"]].strip()
        date_token = release_date_text.lower()
        time_token = release_time_text.lower()

        if date_token in _CANCELED_TOKENS or time_token in _CANCELED_TOKENS:
            candidates.append(
                ScheduleCandidate(
                    artifact_content_sha256=hashlib.sha256(body).hexdigest(),
                    reference_month=reference_month,
                    schedule_status=ScheduleStatus.CANCELED,
                    scheduled_date=None,
                    scheduled_at=None,
                    schedule_timezone=None,
                    time_precision=None,
                    source_role=SourceAuthorityRole.AUTHORITATIVE,
                    extractor_contract_version=extractor_contract_version,
                )
            )
            continue

        if date_token in _PENDING_TOKENS:
            candidates.append(
                ScheduleCandidate(
                    artifact_content_sha256=hashlib.sha256(body).hexdigest(),
                    reference_month=reference_month,
                    schedule_status=ScheduleStatus.DATE_PENDING,
                    scheduled_date=None,
                    scheduled_at=None,
                    schedule_timezone=None,
                    time_precision=None,
                    source_role=SourceAuthorityRole.AUTHORITATIVE,
                    extractor_contract_version=extractor_contract_version,
                )
            )
            continue

        release_date = _parse_release_date(release_date_text)
        if not release_time_text:
            candidates.append(
                ScheduleCandidate(
                    artifact_content_sha256=hashlib.sha256(body).hexdigest(),
                    reference_month=reference_month,
                    schedule_status=ScheduleStatus.SCHEDULED,
                    scheduled_date=release_date,
                    scheduled_at=None,
                    schedule_timezone=timezone_name,
                    time_precision=TimePrecision.DATE_ONLY,
                    source_role=SourceAuthorityRole.AUTHORITATIVE,
                    extractor_contract_version=extractor_contract_version,
                )
            )
            continue

        scheduled_at = _parse_release_time(
            release_time_text,
            release_date=release_date,
            timezone_name=timezone_name,
        )
        candidates.append(
            ScheduleCandidate(
                artifact_content_sha256=hashlib.sha256(body).hexdigest(),
                reference_month=reference_month,
                schedule_status=ScheduleStatus.SCHEDULED,
                scheduled_date=release_date,
                scheduled_at=scheduled_at,
                schedule_timezone=timezone_name,
                time_precision=TimePrecision.EXACT,
                source_role=SourceAuthorityRole.AUTHORITATIVE,
                extractor_contract_version=extractor_contract_version,
            )
        )

    if len(candidates) != 1:
        raise ScheduleParseError(
            "expected exactly one CPI schedule row for the requested reference month"
        )
    return candidates[0]


def _is_explicit_canceled_cell(value: str) -> bool:
    normalized = " ".join(value.split()).lower()
    return re.fullmatch(
        r"cancell?ed(?:\s*\(see\s+.*\s+note\))?",
        normalized,
    ) is not None


def parse_bls_revised_release_dates_html(
    body: bytes,
    *,
    expected_reference_month: date,
    extractor_contract_version: str = "bls-cpi-revised-release-dates-v1",
) -> ScheduleCandidate:
    """Parse explicit CPI cancellation from the BLS lapse revised-dates surface."""

    if len(body) > _MAX_SCHEDULE_HTML_BYTES:
        raise ScheduleParseError("BLS revised release dates HTML exceeds parser byte limit")
    if expected_reference_month.day != 1:
        raise ValueError("expected_reference_month must be the first day of the month")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ScheduleParseError(
            "BLS revised release dates HTML must be UTF-8"
        ) from exc

    parser = _SemanticTableParser()
    parser.feed(text)
    required = (
        "release",
        "reference period",
        "previously scheduled release date",
        "revised release date",
        "time",
    )
    matches: list[ScheduleCandidate] = []

    for rows in parser.tables:
        if not rows:
            continue
        headers = [_normalize_header(cell) for cell in rows[0]]
        if not all(header in headers for header in required):
            continue
        indexes = {header: headers.index(header) for header in required}
        needed = max(indexes.values())

        for row in rows[1:]:
            if len(row) <= needed:
                continue
            if _normalize_header(row[indexes["release"]]) != "consumer price index":
                continue
            try:
                reference_month = _parse_reference_month(
                    row[indexes["reference period"]]
                )
            except ScheduleParseError:
                continue
            if reference_month != expected_reference_month:
                continue
            revised = row[indexes["revised release date"]]
            if not _is_explicit_canceled_cell(revised):
                raise ScheduleParseError(
                    "matching CPI exception row is not explicitly canceled"
                )
            matches.append(
                ScheduleCandidate(
                    artifact_content_sha256=hashlib.sha256(body).hexdigest(),
                    reference_month=reference_month,
                    schedule_status=ScheduleStatus.CANCELED,
                    scheduled_date=None,
                    scheduled_at=None,
                    schedule_timezone=None,
                    time_precision=None,
                    source_role=SourceAuthorityRole.AUTHORITATIVE,
                    extractor_contract_version=extractor_contract_version,
                )
            )

    if len(matches) != 1:
        raise ScheduleParseError(
            "expected exactly one explicitly canceled CPI exception row"
        )
    return matches[0]


def _unfold_ics_lines(text: str) -> list[str]:
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if len(lines) > _MAX_ICS_LINES:
        raise ScheduleParseError("BLS ICS exceeds line-count limit")
    unfolded: list[str] = []
    for line in lines:
        if len(line) > _MAX_ICS_LINE_CHARS:
            raise ScheduleParseError("BLS ICS line exceeds text limit")
        if line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)
    return unfolded


def parse_cpi_schedule_ics(
    body: bytes,
    *,
    expected_reference_month: date,
    extractor_contract_version: str,
) -> ScheduleCandidate:
    if len(body) > _MAX_ICS_BYTES:
        raise ScheduleParseError("BLS ICS exceeds parser byte limit")
    if expected_reference_month.day != 1:
        raise ValueError("expected_reference_month must be the first day of the month")
    try:
        lines = _unfold_ics_lines(body.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ScheduleParseError("BLS ICS must be UTF-8") from exc

    events: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in lines:
        if line == "BEGIN:VEVENT":
            if len(events) >= _MAX_ICS_EVENTS:
                raise ScheduleParseError("BLS ICS exceeds event-count limit")
            current = {}
            continue
        if line == "END:VEVENT":
            if current is not None:
                events.append(current)
            current = None
            continue
        if current is None or ":" not in line:
            continue
        key, value = line.split(":", 1)
        current[key] = value.strip()

    wanted: list[ScheduleCandidate] = []
    for event in events:
        summary = event.get("SUMMARY", "")
        match = re.fullmatch(
            r"Consumer Price Index(?:\s+for)?\s+([A-Za-z]+)\s+(\d{4})",
            " ".join(summary.split()),
            flags=re.IGNORECASE,
        )
        if not match:
            continue
        reference_month = _parse_reference_month(f"{match.group(1)} {match.group(2)}")
        if reference_month != expected_reference_month:
            continue

        dtstart_key = next((key for key in event if key.startswith("DTSTART")), None)
        if dtstart_key is None:
            raise ScheduleParseError("CPI ICS event is missing DTSTART")
        value = event[dtstart_key]
        timezone_match = re.search(r"(?:^|;)TZID=([^;:]+)", dtstart_key)
        timezone_name = timezone_match.group(1) if timezone_match else None

        if re.fullmatch(r"\d{8}", value):
            release_date = datetime.strptime(value, "%Y%m%d").date()
            wanted.append(
                ScheduleCandidate(
                    artifact_content_sha256=hashlib.sha256(body).hexdigest(),
                    reference_month=reference_month,
                    schedule_status=ScheduleStatus.SCHEDULED,
                    scheduled_date=release_date,
                    scheduled_at=None,
                    schedule_timezone=timezone_name or "America/New_York",
                    time_precision=TimePrecision.DATE_ONLY,
                    source_role=SourceAuthorityRole.FALLBACK_CORROBORATION,
                    extractor_contract_version=extractor_contract_version,
                )
            )
            continue

        if timezone_name is None:
            raise ScheduleParseError("exact CPI ICS DTSTART requires explicit TZID")
        try:
            naive = datetime.strptime(value, "%Y%m%dT%H%M%S")
        except ValueError as exc:
            raise ScheduleParseError("unsupported CPI ICS DTSTART format") from exc
        scheduled_at = naive.replace(tzinfo=ZoneInfo(timezone_name))
        wanted.append(
            ScheduleCandidate(
                artifact_content_sha256=hashlib.sha256(body).hexdigest(),
                reference_month=reference_month,
                schedule_status=ScheduleStatus.SCHEDULED,
                scheduled_date=scheduled_at.date(),
                scheduled_at=scheduled_at,
                schedule_timezone=timezone_name,
                time_precision=TimePrecision.EXACT,
                source_role=SourceAuthorityRole.FALLBACK_CORROBORATION,
                extractor_contract_version=extractor_contract_version,
            )
        )

    if len(wanted) != 1:
        raise ScheduleParseError(
            "expected exactly one CPI ICS event for the requested reference month"
        )
    return wanted[0]
