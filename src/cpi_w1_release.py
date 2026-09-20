"""Deterministic CPI release-envelope and Core 4 HTML extraction."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from typing import Iterable
from zoneinfo import ZoneInfo

from src.cpi_w1_contracts import ObservationMaterial, ObservationState, TimePrecision


_RELEASE_TZ = "America/New_York"
_MONTH_NAMES = (
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
)


class ReleaseEnvelopeError(ValueError):
    pass


class ExpectedReleaseNotVisible(ReleaseEnvelopeError):
    """The current mutable locator does not describe the expected occurrence."""


class ObservationExtractionError(ValueError):
    pass


@dataclass(frozen=True)
class ReleaseEnvelopeCandidate:
    event_type: str
    reference_month: date
    marker_date: date
    marker_at: datetime
    marker_timezone: str
    time_precision: TimePrecision
    extractor_contract_version: str


@dataclass(frozen=True)
class ObservationCandidate:
    material: ObservationMaterial
    source_value_text: str
    source_reason_text: str | None = None

    @property
    def observation_code(self) -> str:
        return self.material.observation_code

    @property
    def assertion_state(self) -> ObservationState:
        return self.material.assertion_state

    @property
    def normalized_value(self) -> Decimal | None:
        return self.material.normalized_value

    @property
    def material_fingerprint(self) -> str:
        return self.material.fingerprint


@dataclass(frozen=True)
class ObservationBundleCandidate:
    reference_month: date
    observations: tuple[ObservationCandidate, ...]
    extractor_contract_version: str

    def by_code(self) -> dict[str, ObservationCandidate]:
        return {item.observation_code: item for item in self.observations}


@dataclass(frozen=True)
class _Cell:
    text: str
    is_header: bool
    rowspan: int
    colspan: int


@dataclass(frozen=True)
class _Table:
    rows: tuple[tuple[_Cell, ...], ...]


class _ReleaseHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.text_parts: list[str] = []
        self.tables: list[_Table] = []
        self._table_depth = 0
        self._rows: list[tuple[_Cell, ...]] | None = None
        self._row: list[_Cell] | None = None
        self._cell_parts: list[str] | None = None
        self._cell_header = False
        self._cell_rowspan = 1
        self._cell_colspan = 1

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        attrs_map = {key.lower(): value for key, value in attrs}
        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._rows = []
        elif self._table_depth == 1 and tag == "tr":
            self._row = []
        elif self._table_depth == 1 and tag in {"th", "td"}:
            self._cell_parts = []
            self._cell_header = tag == "th"
            self._cell_rowspan = _positive_int(attrs_map.get("rowspan"), default=1)
            self._cell_colspan = _positive_int(attrs_map.get("colspan"), default=1)

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.text_parts.append(data)
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self._table_depth == 1 and tag in {"th", "td"} and self._cell_parts is not None:
            text = " ".join("".join(self._cell_parts).split())
            if self._row is not None:
                self._row.append(
                    _Cell(
                        text=text,
                        is_header=self._cell_header,
                        rowspan=self._cell_rowspan,
                        colspan=self._cell_colspan,
                    )
                )
            self._cell_parts = None
        elif self._table_depth == 1 and tag == "tr":
            if self._rows is not None and self._row:
                self._rows.append(tuple(self._row))
            self._row = None
        elif tag == "table" and self._table_depth:
            if self._table_depth == 1 and self._rows is not None:
                self.tables.append(_Table(rows=tuple(self._rows)))
                self._rows = None
            self._table_depth -= 1


def _positive_int(value: str | None, *, default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ReleaseEnvelopeError("invalid table span") from exc
    if parsed < 1 or parsed > 100:
        raise ReleaseEnvelopeError("table span outside safe bounds")
    return parsed


def _normalized_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _semantic_text(value: str) -> str:
    value = _normalized_text(value).lower()
    value = re.sub(r"\(\d+\)\s*$", "", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def _reference_month_from_heading(full_text: str) -> date:
    match = re.search(
        r"CONSUMER\s+PRICE\s+INDEX\s*[-–—]\s*([A-Z]+)\s+(\d{4})",
        full_text,
        flags=re.IGNORECASE,
    )
    if not match:
        raise ReleaseEnvelopeError("CPI release heading not recognized")
    month_token = match.group(1).lower()
    if month_token not in _MONTH_NAMES:
        raise ReleaseEnvelopeError("CPI release heading month not recognized")
    return date(int(match.group(2)), _MONTH_NAMES.index(month_token) + 1, 1)


def _parse_marker(full_text: str) -> tuple[date, datetime]:
    match = re.search(
        r"embargoed\s+until\s+"
        r"(\d{1,2}):(\d{2})\s*([ap])\.?m\.?\s*"
        r"\(ET\)\s*(?:[A-Za-z]+,\s*)?"
        r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})",
        full_text,
        flags=re.IGNORECASE,
    )
    if not match:
        raise ReleaseEnvelopeError("official CPI release marker not recognized")

    hour = int(match.group(1))
    minute = int(match.group(2))
    if not 1 <= hour <= 12 or not 0 <= minute <= 59:
        raise ReleaseEnvelopeError("official CPI release marker time is invalid")
    if match.group(3).lower() == "p" and hour != 12:
        hour += 12
    elif match.group(3).lower() == "a" and hour == 12:
        hour = 0

    month_token = match.group(4).lower()
    if month_token not in _MONTH_NAMES:
        raise ReleaseEnvelopeError("official CPI release marker month is invalid")
    marker_date = date(
        int(match.group(6)),
        _MONTH_NAMES.index(month_token) + 1,
        int(match.group(5)),
    )
    marker_at = datetime(
        marker_date.year,
        marker_date.month,
        marker_date.day,
        hour,
        minute,
        tzinfo=ZoneInfo(_RELEASE_TZ),
    )
    return marker_date, marker_at


def _expand_table(table: _Table) -> list[list[tuple[str, bool]]]:
    grid: list[list[tuple[str, bool]]] = []
    active: dict[int, tuple[int, str, bool]] = {}

    for source_row in table.rows:
        row: list[tuple[str, bool]] = []
        col = 0

        def fill_active() -> None:
            nonlocal col
            while col in active:
                remaining, text, is_header = active[col]
                row.append((text, is_header))
                if remaining <= 1:
                    del active[col]
                else:
                    active[col] = (remaining - 1, text, is_header)
                col += 1

        for cell in source_row:
            fill_active()
            for offset in range(cell.colspan):
                row.append((cell.text, cell.is_header))
                if cell.rowspan > 1:
                    active[col + offset] = (
                        cell.rowspan - 1,
                        cell.text,
                        cell.is_header,
                    )
            col += cell.colspan

        fill_active()
        grid.append(row)

    return grid


def _table_signature(grid: list[list[tuple[str, bool]]]) -> str:
    values = []
    for row in grid[:8]:
        values.extend(text for text, is_header in row if is_header and text)
    return _semantic_text(" ".join(values))


def _find_table1(parser: _ReleaseHtmlParser) -> list[list[tuple[str, bool]]]:
    matches: list[list[list[tuple[str, bool]]]] = []
    for table in parser.tables:
        grid = _expand_table(table)
        signature = _table_signature(grid)
        required = (
            "expenditure category",
            "unadjusted indexes",
            "unadjusted percent change",
            "seasonally adjusted percent change",
        )
        if all(item in signature for item in required):
            matches.append(grid)
    if len(matches) != 1:
        raise ReleaseEnvelopeError(
            "expected exactly one CPI Table 1 semantic surface"
        )
    return matches[0]


def _header_rows(grid: list[list[tuple[str, bool]]]) -> list[list[tuple[str, bool]]]:
    rows = []
    for row in grid:
        nonempty = [(text, is_header) for text, is_header in row if text]
        if not nonempty:
            continue
        if all(is_header for _, is_header in nonempty):
            rows.append(row)
            continue
        break
    if not rows:
        raise ObservationExtractionError("Table 1 header rows not recognized")
    return rows


def _column_paths(header_rows: list[list[tuple[str, bool]]]) -> dict[int, str]:
    width = max(len(row) for row in header_rows)
    result: dict[int, str] = {}
    for column in range(width):
        parts: list[str] = []
        for row in header_rows:
            if column < len(row):
                text = row[column][0]
                if text and (not parts or parts[-1] != text):
                    parts.append(text)
        result[column] = _semantic_text(" ".join(parts))
    return result


def _month_token(value: date) -> tuple[str, str]:
    return _MONTH_NAMES[value.month - 1][:3], str(value.year)


def _previous_month(value: date) -> date:
    if value.month == 1:
        return date(value.year - 1, 12, 1)
    return date(value.year, value.month - 1, 1)


def _column_for_yoy(paths: dict[int, str], reference_month: date) -> int:
    current_month, current_year = _month_token(reference_month)
    prior_year = str(reference_month.year - 1)
    matches = [
        column
        for column, path in paths.items()
        if "unadjusted percent change" in path
        and current_month in path
        and current_year in path
        and prior_year in path
    ]
    if len(matches) != 1:
        raise ObservationExtractionError("unique CPI YoY column not recognized")
    return matches[0]


def _column_for_mom(paths: dict[int, str], reference_month: date) -> int:
    current_month, current_year = _month_token(reference_month)
    previous = _previous_month(reference_month)
    previous_month, previous_year = _month_token(previous)
    matches = [
        column
        for column, path in paths.items()
        if "seasonally adjusted percent change" in path
        and current_month in path
        and current_year in path
        and previous_month in path
        and previous_year in path
    ]
    if len(matches) != 1:
        raise ObservationExtractionError("unique CPI MoM column not recognized")
    return matches[0]


def _data_rows(
    grid: list[list[tuple[str, bool]]],
    header_count: int,
) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    for row in grid[header_count:]:
        values = [text for text, _ in row]
        if not values or not values[0]:
            continue
        label = _semantic_text(values[0])
        if label in {"all items", "all items less food and energy"}:
            if label in rows:
                raise ObservationExtractionError(f"duplicate Table 1 row: {label}")
            rows[label] = values
    return rows


def _explicit_unavailable_reason(
    full_text: str,
    reference_month: date,
) -> str | None:
    previous = _previous_month(reference_month)
    previous_name = _MONTH_NAMES[previous.month - 1]
    normalized = _semantic_text(full_text)
    has_period = previous_name in normalized and str(previous.year) in normalized
    has_unavailability = (
        "did not collect" in normalized
        or "data values are not available" in normalized
        or "unable to retroactively collect" in normalized
    )
    has_official_reason = (
        "lapse in appropriations" in normalized
        or "lapse in federal appropriations" in normalized
    )
    if not (has_period and has_unavailability and has_official_reason):
        return None
    return (
        "BLS official release states "
        f"{_MONTH_NAMES[previous.month - 1].title()} {previous.year} CPI source data "
        "were unavailable due to a lapse in appropriations."
    )


def _candidate_from_cell(
    *,
    code: str,
    raw_value: str,
    explicit_unavailable_reason: str | None,
) -> ObservationCandidate:
    cleaned = _normalized_text(raw_value)
    if cleaned in {"-", "—", "–", ""}:
        if explicit_unavailable_reason is None:
            raise ObservationExtractionError(
                f"{code} is nonnumeric without explicit official unavailability context"
            )
        return ObservationCandidate(
            material=ObservationMaterial(
                observation_code=code,
                assertion_state=ObservationState.EXPLICIT_UNAVAILABLE,
                normalized_value=None,
            ),
            source_value_text=cleaned,
            source_reason_text=explicit_unavailable_reason,
        )
    try:
        value = Decimal(cleaned.replace("%", "").strip())
    except InvalidOperation as exc:
        raise ObservationExtractionError(
            f"{code} value is not a canonical decimal"
        ) from exc
    if not value.is_finite():
        raise ObservationExtractionError(f"{code} value must be finite")
    return ObservationCandidate(
        material=ObservationMaterial(
            observation_code=code,
            assertion_state=ObservationState.VALUE,
            normalized_value=value,
        ),
        source_value_text=cleaned,
        source_reason_text=None,
    )


def _parse(body: bytes) -> tuple[_ReleaseHtmlParser, str]:
    if len(body) > 8 * 1024 * 1024:
        raise ReleaseEnvelopeError("CPI release HTML exceeds parser byte limit")
    try:
        html = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReleaseEnvelopeError("CPI release HTML must be UTF-8") from exc
    parser = _ReleaseHtmlParser()
    parser.feed(html)
    full_text = _normalized_text(" ".join(parser.text_parts))
    return parser, full_text


def extract_release_envelope(
    body: bytes,
    *,
    expected_reference_month: date,
    extractor_contract_version: str = "bls-cpi-release-html-v1",
) -> ReleaseEnvelopeCandidate:
    if expected_reference_month.day != 1:
        raise ValueError("expected_reference_month must be first day of month")
    parser, full_text = _parse(body)
    actual_reference_month = _reference_month_from_heading(full_text)
    if actual_reference_month != expected_reference_month:
        raise ExpectedReleaseNotVisible(
            "current CPI release does not match expected reference month"
        )
    marker_date, marker_at = _parse_marker(full_text)
    _find_table1(parser)
    return ReleaseEnvelopeCandidate(
        event_type="CPI",
        reference_month=actual_reference_month,
        marker_date=marker_date,
        marker_at=marker_at,
        marker_timezone=_RELEASE_TZ,
        time_precision=TimePrecision.EXACT,
        extractor_contract_version=extractor_contract_version,
    )


def extract_core4_from_release_html(
    body: bytes,
    *,
    expected_reference_month: date,
    extractor_contract_version: str = "bls-cpi-release-html-v1",
) -> ObservationBundleCandidate:
    envelope = extract_release_envelope(
        body,
        expected_reference_month=expected_reference_month,
        extractor_contract_version=extractor_contract_version,
    )
    parser, full_text = _parse(body)
    grid = _find_table1(parser)
    headers = _header_rows(grid)
    paths = _column_paths(headers)
    yoy_col = _column_for_yoy(paths, envelope.reference_month)
    mom_col = _column_for_mom(paths, envelope.reference_month)
    rows = _data_rows(grid, len(headers))

    headline = rows.get("all items")
    core = rows.get("all items less food and energy")
    if headline is None or core is None:
        raise ObservationExtractionError("required CPI Core 4 rows are missing")
    max_col = max(yoy_col, mom_col)
    if len(headline) <= max_col or len(core) <= max_col:
        raise ObservationExtractionError("required CPI Core 4 cells are missing")

    unavailable_reason = _explicit_unavailable_reason(
        full_text,
        envelope.reference_month,
    )
    observations = (
        _candidate_from_cell(
            code="CPI_HEADLINE_MOM",
            raw_value=headline[mom_col],
            explicit_unavailable_reason=unavailable_reason,
        ),
        _candidate_from_cell(
            code="CPI_HEADLINE_YOY",
            raw_value=headline[yoy_col],
            explicit_unavailable_reason=None,
        ),
        _candidate_from_cell(
            code="CPI_CORE_MOM",
            raw_value=core[mom_col],
            explicit_unavailable_reason=unavailable_reason,
        ),
        _candidate_from_cell(
            code="CPI_CORE_YOY",
            raw_value=core[yoy_col],
            explicit_unavailable_reason=None,
        ),
    )
    if {item.observation_code for item in observations} != {
        "CPI_HEADLINE_MOM",
        "CPI_HEADLINE_YOY",
        "CPI_CORE_MOM",
        "CPI_CORE_YOY",
    }:
        raise ObservationExtractionError("Core 4 observation contract is incomplete")
    return ObservationBundleCandidate(
        reference_month=envelope.reference_month,
        observations=observations,
        extractor_contract_version=extractor_contract_version,
    )
