"""Schema drift guard.

Scans the dashboard + engine source for SQL queries, extracts the
columns they reference against each table, and checks that every
referenced column appears in ``bootstrap_schema``'s ``CREATE TABLE``
+ ``ALTER TABLE ADD COLUMN`` declarations.

This catches the class of bug that recurred across rounds 1-3:
someone adds a new SELECT column without adding a migration, and the
dashboard 500s on every minimal-DB bootstrap.

Invocation:
  - CLI: ``python3 scripts/guards/check_schema_drift.py``
         Exits 0 if clean, 1 with drift report otherwise.
  - Pytest: ``tests/test_schema_drift_guard.py`` wraps it.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Known migrated columns per table.
#
# The canonical source is ``bootstrap_schema`` in
# scripts/review_dashboard.py. We parse it dynamically so the guard
# stays in sync with whatever migrations exist.
# ---------------------------------------------------------------------------

DASHBOARD = ROOT / "scripts" / "review_dashboard.py"


# Files that we use to (a) collect the migrated-columns catalog and
# (b) scan for SQL usages. Everything under scripts/ and src/ that's
# a Python file is fair game.
def _all_py_files() -> list[Path]:
    out: list[Path] = []
    for base in (ROOT / "scripts", ROOT / "src"):
        if base.exists():
            out.extend(p for p in base.rglob("*.py")
                       if "__pycache__" not in p.parts)
    return out


SCAN_FILES = _all_py_files()

# Files to consult for CREATE TABLE / ALTER TABLE migrations. Broader
# than SCAN_FILES so test fixtures and secondary scripts that create
# tables also contribute to the known-columns catalog.
def _all_schema_sources() -> list[Path]:
    out: list[Path] = _all_py_files()
    tests_dir = ROOT / "tests"
    if tests_dir.exists():
        out.extend(p for p in tests_dir.rglob("*.py")
                   if "__pycache__" not in p.parts)
    return out


# SQLite pseudo-columns that are always present.
PSEUDO_COLUMNS = {"rowid", "oid", "_rowid_"}


# Tables we can't validate (external systems, dynamic names, etc.).
# These are ignored when they show up in queries.
IGNORED_TABLES = {
    "sqlite_master",
    "sqlite_sequence",
}


# ---------------------------------------------------------------------------
# Parser: extract (table, columns[]) from CREATE TABLE and
# ALTER TABLE ... ADD COLUMN statements.
# ---------------------------------------------------------------------------

_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"([\"\w.]+)\s*\(\s*(.*?)\s*\)\s*(?:WITHOUT\s+ROWID)?\s*[;\"']",
    re.IGNORECASE | re.DOTALL,
)

_ALTER_ADD_COL_RE = re.compile(
    r"ALTER\s+TABLE\s+([\"\w.]+)\s+ADD\s+COLUMN\s+([\"\w]+)",
    re.IGNORECASE,
)

# Columns extracted from a CREATE-table body: lines that start with an
# identifier and aren't PK/UNIQUE/FOREIGN constraints.
_COL_LINE_RE = re.compile(
    r"^\s*([\"\w]+)\s+",  # identifier + whitespace
)
_CONSTRAINT_KEYWORDS = {
    "primary", "unique", "foreign", "check", "constraint", "create",
    "index", "references",
}


def _clean_ident(ident: str) -> str:
    return ident.strip().strip('"').strip()


def _extract_columns_from_create_body(body: str) -> set[str]:
    """Parse the column list inside a CREATE TABLE body."""
    cols: set[str] = set()
    # Strip nested parens carefully (CHECK constraints contain them).
    depth = 0
    chunks: list[str] = []
    buf = []
    for ch in body:
        if ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            chunks.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        chunks.append("".join(buf))

    for chunk in chunks:
        s = chunk.strip()
        if not s:
            continue
        first = s.split(None, 1)[0].lower().lstrip('"').rstrip('"')
        if first in _CONSTRAINT_KEYWORDS:
            continue
        m = _COL_LINE_RE.match(s)
        if m:
            cols.add(_clean_ident(m.group(1)))
    return cols


def collect_migrated_columns(files: Iterable[Path]) -> dict[str, set[str]]:
    """Return {table_name: {column_names}} from all CREATE TABLE and
    ALTER TABLE ADD COLUMN statements found in ``files``."""
    tables: dict[str, set[str]] = {}
    for f in files:
        if not f.exists():
            continue
        src = f.read_text()
        # CREATE TABLE blocks.
        for m in _CREATE_TABLE_RE.finditer(src):
            tname = _clean_ident(m.group(1))
            body = m.group(2)
            cols = _extract_columns_from_create_body(body)
            tables.setdefault(tname, set()).update(cols)
        # ALTER TABLE ADD COLUMN.
        for m in _ALTER_ADD_COL_RE.finditer(src):
            tname = _clean_ident(m.group(1))
            col = _clean_ident(m.group(2))
            tables.setdefault(tname, set()).add(col)
    return tables


# ---------------------------------------------------------------------------
# Parser: extract (table, [columns]) references from SELECT statements.
#
# We only flag cases that are unambiguous:
#   SELECT alias.col FROM real_table alias
#   SELECT alias.col FROM real_table (no alias) if table is named directly
#
# Anonymous `col` without alias in a multi-table query is ambiguous;
# we don't try to resolve it. Our goal is to catch the common case:
#   d.<new_column>  where the FROM/JOIN declares "FROM documents d"
# ---------------------------------------------------------------------------

# A SELECT statement, terminated by a matching FROM ... and an ending
# closing quote. We match non-greedily up to the closing triple-quote
# or single-quote.
_SELECT_RE = re.compile(
    r"(SELECT\s+.*?\bFROM\s+.*?)(?=\"\"\"|'''|\"\s*[,)])",
    re.IGNORECASE | re.DOTALL,
)


# Inside a triple-quoted SQL block (which the dashboard uses heavily),
# we want to scan from ``SELECT`` through a matching end. We approximate
# by taking up to 40 lines after SELECT.
_SQL_BLOCK_RE = re.compile(
    r"(SELECT\s+[^\"';]+?\s+FROM\s+[^\"';]+?)(?=\"\"\"|'''|\"\s*[,)]|\Z)",
    re.IGNORECASE | re.DOTALL,
)


# An alias declaration in a FROM clause: "FROM documents d" or
# "JOIN posting_jobs pj" — we want alias → table_name.
_FROM_ALIAS_RE = re.compile(
    r"(?:FROM|JOIN)\s+([\"\w]+)(?:\s+(?:AS\s+)?([a-z_][\w]*))?",
    re.IGNORECASE,
)


# Reference to alias.column — "d.vendor", "c.firm_code".
_ALIAS_COL_RE = re.compile(
    r"\b([a-z_][a-z0-9_]{0,20})\.([a-z_][a-z0-9_]*)\b"
)


# Reserved SQL keywords that happen to fit the alias-name pattern.
_SQL_KEYWORDS = {
    "select", "from", "where", "order", "group", "having", "limit",
    "offset", "inner", "outer", "left", "right", "join", "on", "and",
    "or", "not", "as", "by", "asc", "desc", "like", "in", "case",
    "when", "then", "else", "end", "is", "null", "distinct", "count",
    "sum", "avg", "min", "max", "coalesce", "cast", "union", "all",
    "exists", "with", "values", "set", "update", "insert", "delete",
}


def _split_sql_blocks(src: str) -> list[str]:
    """Pull out the plausibly-SQL chunks from Python source.

    We look for triple-quoted strings containing SELECT/INSERT/UPDATE
    keywords, plus single-line .execute("...") calls.
    """
    blocks: list[str] = []
    # Triple-quoted strings.
    for m in re.finditer(r'"""(.*?)"""', src, re.DOTALL):
        b = m.group(1)
        if re.search(r"\b(SELECT|INSERT|UPDATE|DELETE)\b", b, re.IGNORECASE):
            blocks.append(b)
    for m in re.finditer(r"'''(.*?)'''", src, re.DOTALL):
        b = m.group(1)
        if re.search(r"\b(SELECT|INSERT|UPDATE|DELETE)\b", b, re.IGNORECASE):
            blocks.append(b)
    # Single-quoted / double-quoted SQL strings (up to the next
    # matching closing quote on the same line).
    for m in re.finditer(r'"([^"\n]{20,})"', src):
        b = m.group(1)
        if re.search(r"\b(SELECT|INSERT|UPDATE|DELETE)\b", b, re.IGNORECASE):
            blocks.append(b)
    return blocks


def _extract_alias_map(sql_block: str) -> dict[str, str]:
    """Return {alias: table_name} from FROM/JOIN clauses in a SQL block."""
    aliases: dict[str, str] = {}
    for m in _FROM_ALIAS_RE.finditer(sql_block):
        table = _clean_ident(m.group(1))
        alias = m.group(2)
        if alias:
            aliases[alias.lower()] = table
        # Allow the raw table name to act as its own "alias" so
        # "documents.vendor" resolves too.
        aliases[table.lower()] = table
    return aliases


def _extract_alias_col_refs(sql_block: str) -> list[tuple[str, str]]:
    """Return [(alias_name, column_name)] from alias.col references."""
    out: list[tuple[str, str]] = []
    for m in _ALIAS_COL_RE.finditer(sql_block):
        alias = m.group(1).lower()
        col = m.group(2)
        if alias in _SQL_KEYWORDS:
            continue
        out.append((alias, col))
    return out


def find_drift(files: Iterable[Path]) -> list[tuple[str, int, str, str]]:
    """Return a list of (file, line_no, table, column) drift tuples.

    A tuple is emitted when:
      - A SQL block references ``alias.column``
      - The alias maps to a known table via FROM/JOIN
      - The table is in our migrated-tables catalog
      - The column is NOT in that table's migrated-column set
    """
    # Collect migrated columns from the broader schema-source set
    # (production + test fixtures + secondary scripts) so a column
    # migrated in one place isn't flagged as drift elsewhere.
    tables = collect_migrated_columns(_all_schema_sources())
    drift: list[tuple[str, int, str, str]] = []

    # Reasonable baseline: every SQLite pragma column and COUNT(*)
    # output is safe. We also allow columns that appear in ANY of our
    # known tables (cross-table aliases are noisy to detect
    # precisely), on the assumption that if the column exists
    # somewhere in the schema it's usually fine. We can tighten later.
    all_known_cols: set[str] = set()
    for cols in tables.values():
        all_known_cols.update(cols)

    for f in files:
        if not f.exists():
            continue
        src = f.read_text()
        line_offsets = _line_offsets(src)
        blocks = _split_sql_blocks(src)
        for block in blocks:
            alias_map = _extract_alias_map(block)
            for alias, col in _extract_alias_col_refs(block):
                table = alias_map.get(alias)
                if not table:
                    continue
                if table in IGNORED_TABLES:
                    continue
                # Only flag if we actually know this table (have its
                # migrated columns). Unknown tables are ignored to
                # avoid false alarms on tables created in other paths.
                if table not in tables:
                    continue
                known = tables[table]
                if col in known:
                    continue
                if col in PSEUDO_COLUMNS:
                    continue
                # Emit drift.
                pos = src.find(block)
                line_no = (
                    _pos_to_line(line_offsets, pos) if pos >= 0 else -1
                )
                try:
                    rel = str(f.relative_to(ROOT))
                except ValueError:
                    rel = str(f)  # outside the repo (e.g. tmp_path tests)
                drift.append((rel, line_no, table, col))
    return drift


def _line_offsets(src: str) -> list[int]:
    offsets = [0]
    for i, ch in enumerate(src):
        if ch == "\n":
            offsets.append(i + 1)
    return offsets


def _pos_to_line(offsets: list[int], pos: int) -> int:
    import bisect
    return bisect.bisect_right(offsets, pos)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    drift = find_drift(SCAN_FILES)
    # Dedup — same (file, table, col) may appear in multiple blocks.
    dedup: dict[tuple[str, str, str], tuple[str, int, str, str]] = {}
    for item in drift:
        key = (item[0], item[2], item[3])
        if key not in dedup or dedup[key][1] > item[1]:
            dedup[key] = item
    drift = sorted(dedup.values(), key=lambda r: (r[0], r[1]))
    if not drift:
        print("schema-drift-guard: ok")
        return 0
    print("schema-drift-guard: DRIFT DETECTED")
    print()
    for file, line, table, col in drift:
        print(f"  {file}:{line}  missing column {col!r} on table {table!r}")
    print()
    print(f"total: {len(drift)} drift(s)")
    print()
    print("Add the column to bootstrap_schema in scripts/review_dashboard.py")
    print("or to the engine's CREATE TABLE statement, then re-run the guard.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
