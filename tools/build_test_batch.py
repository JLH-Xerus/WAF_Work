"""
build_test_batch.py
-------------------
Convert a stored procedure DDL file (CREATE PROCEDURE ...) into a runnable
test batch suitable for the ProcBenchmark harness.

What it does:
    1. Walks refactors/<ProcName>/ folders.
    2. For each, reads Original.sql and Refactored.sql.
    3. Strips the CREATE/ALTER PROCEDURE shell.
    4. Converts the parameter list into top-of-batch DECLARE statements,
       using the proc's default values where present.
    5. Comments out Exec lsp_DbLogSqlEvent calls (no execute permission
       on the workstation running the harness).
    6. Writes Original.batch.sql and Refactored.batch.sql alongside the
       originals.
    7. Emits tools/batch_conversion_report.md summarising what was
       converted, what was skipped, and what needs manual attention
       (TVPs, OUTPUT-only procs that need callers, no-default inputs).

Delete-family procs (matching DELETE_PROC_PATTERN below) are skipped by
default per Justin's instruction; they will be revisited once the read-only
procs are validated.

Usage:
    python3 tools/build_test_batch.py
    python3 tools/build_test_batch.py --proc lsp_ImgGetListOfTopXImagesToMove
    python3 tools/build_test_batch.py --include-deletes
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
REFACTORS_DIR = REPO_ROOT / "refactors"
REPORT_PATH = REPO_ROOT / "tools" / "batch_conversion_report.md"

# Procs to skip by default. The delete/purge family contains real DML
# that we don't want to run, even in a BEGIN TRAN, until the read-only
# procs are validated.
DELETE_PROC_PATTERN = re.compile(r"^lsp_(Db(Delete|Purge)|.*Purge)", re.IGNORECASE)


# ----------------------------------------------------------------------
# Data classes
# ----------------------------------------------------------------------

@dataclass
class Parameter:
    name: str            # e.g. "@OlderThanXDays"
    sql_type: str        # e.g. "Int", "VarChar(760)", "dbo.udtTableOfVarChars"
    default: Optional[str] = None     # e.g. "36500", "NULL", "'foo'"
    is_output: bool = False
    is_readonly: bool = False         # table-valued parameter

    @property
    def is_tvp(self) -> bool:
        return self.is_readonly  # READONLY in a proc signature implies TVP


@dataclass
class ConversionResult:
    proc_name: str
    variant: str                                  # "Original" or "Refactored"
    output_path: Optional[Path] = None
    parameters: list[Parameter] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    needs_manual_review: bool = False
    skipped: bool = False
    error: Optional[str] = None


# ----------------------------------------------------------------------
# Parsing helpers
# ----------------------------------------------------------------------

_RE_PREAMBLE_NOISE = re.compile(
    r"^\s*(/\*+\s*Object:.*?\*+/|SET\s+(ANSI_NULLS|QUOTED_IDENTIFIER)\s+(ON|OFF)|GO|--/)\s*$",
    re.IGNORECASE,
)

_RE_PROC_SIG = re.compile(
    r"""^\s*
        (?:CREATE|ALTER)\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+
        (?:                                       # optional schema. prefix
            (?:"[^"]+"|\[[^\]]+\]|\w+)
            \s*\.\s*
        )?
        (?:"[^"]+"|\[[^\]]+\]|\w+)                # proc name
        \s*
    """,
    re.IGNORECASE | re.VERBOSE,
)

_RE_AS_LINE = re.compile(r"^\s*AS\s*(/\*.*)?$", re.IGNORECASE)
_RE_AS_WITH_BEGIN = re.compile(r"^\s*AS\s+BEGIN\s*$", re.IGNORECASE)


def split_top_level_commas(text: str) -> list[str]:
    """Split a parameter list on commas that are not inside parens or quotes."""
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_squote = False
    in_dquote = False
    in_block_comment = False
    in_line_comment = False
    i = 0
    while i < len(text):
        c = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""
        if in_line_comment:
            if c == "\n":
                in_line_comment = False
            buf.append(c)
        elif in_block_comment:
            if c == "*" and nxt == "/":
                in_block_comment = False
                buf.append("*/")
                i += 2
                continue
            buf.append(c)
        elif in_squote:
            buf.append(c)
            if c == "'":
                # SQL escapes '' inside strings
                if nxt == "'":
                    buf.append("'")
                    i += 2
                    continue
                in_squote = False
        elif in_dquote:
            buf.append(c)
            if c == '"':
                in_dquote = False
        else:
            if c == "-" and nxt == "-":
                in_line_comment = True
                buf.append(c)
            elif c == "/" and nxt == "*":
                in_block_comment = True
                buf.append("/*")
                i += 2
                continue
            elif c == "'":
                in_squote = True
                buf.append(c)
            elif c == '"':
                in_dquote = True
                buf.append(c)
            elif c == "(":
                depth += 1
                buf.append(c)
            elif c == ")":
                depth -= 1
                buf.append(c)
            elif c == "," and depth == 0:
                parts.append("".join(buf))
                buf = []
            else:
                buf.append(c)
        i += 1
    if buf:
        parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def strip_inline_comments(text: str) -> str:
    """Remove SQL line and block comments from a single-parameter declaration."""
    # Block comments
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    # Line comments
    text = re.sub(r"--[^\n]*", " ", text)
    return text


def parse_one_parameter(decl: str) -> Optional[Parameter]:
    """Parse a single '@Name Type [= default] [OUTPUT] [READONLY]' fragment."""
    decl = strip_inline_comments(decl).strip()
    if not decl.startswith("@"):
        return None

    # Strip trailing keywords (OUTPUT, OUT, READONLY) before parsing the default
    is_output = False
    is_readonly = False
    # Order matters: READONLY at end, then OUTPUT/OUT
    m_ro = re.search(r"\bREADONLY\b\s*$", decl, re.IGNORECASE)
    if m_ro:
        is_readonly = True
        decl = decl[: m_ro.start()].rstrip()
    m_out = re.search(r"\b(OUTPUT|OUT)\b\s*$", decl, re.IGNORECASE)
    if m_out:
        is_output = True
        decl = decl[: m_out.start()].rstrip()

    # Now decl is "@Name Type [= default]"
    # Split default on first '=' that's not inside parens/quotes
    name_type, default = _split_on_equals(decl)
    name_type = name_type.strip()

    # Parse @Name and Type
    m = re.match(r"^(@\w+)\s+(.+)$", name_type, re.DOTALL)
    if not m:
        return None
    name = m.group(1)
    sql_type = re.sub(r"\s+", " ", m.group(2)).strip()

    return Parameter(
        name=name,
        sql_type=sql_type,
        default=default.strip() if default else None,
        is_output=is_output,
        is_readonly=is_readonly,
    )


def _split_on_equals(decl: str) -> tuple[str, Optional[str]]:
    """Split a parameter declaration on the first top-level '=' (the default-value marker)."""
    depth = 0
    in_squote = False
    in_dquote = False
    for i, c in enumerate(decl):
        nxt = decl[i + 1] if i + 1 < len(decl) else ""
        if in_squote:
            if c == "'" and nxt != "'":
                in_squote = False
            continue
        if in_dquote:
            if c == '"':
                in_dquote = False
            continue
        if c == "'":
            in_squote = True
        elif c == '"':
            in_dquote = True
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "=" and depth == 0:
            return decl[:i], decl[i + 1 :]
    return decl, None


# ----------------------------------------------------------------------
# Main converter
# ----------------------------------------------------------------------

def convert(sql_text: str, proc_name: str, variant: str) -> tuple[str, ConversionResult]:
    """Convert a CREATE PROCEDURE DDL text into a runnable test batch.

    Returns (batch_sql_text, conversion_result).
    """
    result = ConversionResult(proc_name=proc_name, variant=variant)

    lines = sql_text.splitlines()
    n = len(lines)

    # Phase 1: skip preamble until CREATE/ALTER PROCEDURE
    i = 0
    while i < n and not _RE_PROC_SIG.match(lines[i]):
        i += 1
    if i == n:
        result.error = f"No CREATE PROCEDURE line found in {variant}.sql"
        result.skipped = True
        return "", result

    # Phase 2: parse the proc signature line — the rest of that line after the
    # proc name might be the start of the param list (or empty if params are
    # on subsequent lines).
    sig_line = lines[i]
    rest = _RE_PROC_SIG.sub("", sig_line, count=1)
    param_text_buf: list[str] = []
    if rest.strip():
        param_text_buf.append(rest)

    # Phase 3: collect param lines until the standalone AS keyword
    i += 1
    found_as = False
    while i < n:
        if _RE_AS_LINE.match(lines[i]) or _RE_AS_WITH_BEGIN.match(lines[i]):
            found_as = True
            i += 1
            break
        param_text_buf.append(lines[i])
        i += 1

    if not found_as:
        result.error = f"No standalone AS keyword found in {variant}.sql"
        result.skipped = True
        return "", result

    # Phase 4: body = everything after AS
    body_lines = lines[i:]
    body = "\n".join(body_lines).strip()

    # Phase 5: parse parameters
    raw_params = "\n".join(param_text_buf).strip()
    # If the param list is wrapped in parens, strip them
    if raw_params.startswith("("):
        # Find matching close paren
        depth = 0
        end_idx = -1
        for k, ch in enumerate(raw_params):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end_idx = k
                    break
        if end_idx > 0:
            raw_params = raw_params[1:end_idx]

    parameters: list[Parameter] = []
    if raw_params.strip():
        for piece in split_top_level_commas(raw_params):
            p = parse_one_parameter(piece)
            if p:
                parameters.append(p)
    result.parameters = parameters

    # Phase 6: post-process the body
    # 6a: strip trailing batch terminators in order: solitary '/' lines, then GO,
    # then the final outer END that matches the BEGIN after AS.
    # Loop because they can stack ("End\n\nGO\n/\n").
    for _ in range(4):
        prev = body
        body = re.sub(r"(?im)\n?\s*^\s*/\s*$", "", body, count=1).rstrip()
        body = re.sub(r"(?im)\n?\s*^\s*GO\s*$", "", body, count=1).rstrip()
        if body == prev:
            break
    # Drop a single trailing END (matching the BEGIN that often follows AS)
    body = re.sub(r"(?is)\s*\bEND\b\s*$", "", body).rstrip()
    # If the body starts with BEGIN (separated by whitespace), drop that leading BEGIN
    body = re.sub(r"(?is)^\s*BEGIN\b\s*\n?", "", body)

    # 6b: comment out Exec lsp_DbLogSqlEvent (we don't have execute perm)
    log_pattern = re.compile(r"(?im)^(\s*)(Exec(?:ute)?\s+lsp_DbLogSqlEvent\b[^\n]*)$")
    if log_pattern.search(body):
        body = log_pattern.sub(r"\1-- [batch-converted] \2", body)
        result.notes.append("Commented out Exec lsp_DbLogSqlEvent calls (no execute permission).")

    # Phase 7: build the DECLARE block
    declare_lines: list[str] = []
    tvp_seed_lines: list[str] = []
    has_tvp = False
    has_missing_default = False

    for p in parameters:
        if p.is_tvp:
            has_tvp = True
            declare_lines.append(
                f"DECLARE {p.name} {p.sql_type};  -- TVP: populate via INSERT below before running"
            )
            tvp_seed_lines.append(
                f"-- INSERT INTO {p.name} (Value) VALUES (N'TODO');  -- TODO: supply real test values"
            )
            result.notes.append(
                f"Table-valued parameter {p.name} ({p.sql_type}) requires hand-populated test data."
            )
            result.needs_manual_review = True
        elif p.is_output:
            declare_lines.append(
                f"DECLARE {p.name} {p.sql_type};  -- OUTPUT in proc; assigned by body, discarded after batch"
            )
        elif p.default is not None:
            declare_lines.append(f"DECLARE {p.name} {p.sql_type} = {p.default};")
        else:
            has_missing_default = True
            declare_lines.append(
                f"DECLARE {p.name} {p.sql_type} = NULL;  -- TODO: supply test value"
            )
            result.notes.append(
                f"Input parameter {p.name} ({p.sql_type}) has no default in the DDL; defaulted to NULL."
            )
            result.needs_manual_review = True

    # Phase 7b: sanity-check that every @-prefixed identifier referenced in
    # the body is either a parameter we declared, a local declared in the
    # body itself, a built-in (@@xxx), or a cursor / table variable.
    # The point of this check is to catch parser bugs where a proc parameter
    # was dropped — the body then references @x with no matching DECLARE.
    declared_names = {p.name.lstrip("@").lower() for p in parameters}

    # Strip comments + strings from the body for a clean scan
    body_for_scan = re.sub(r"/\*.*?\*/", " ", body, flags=re.DOTALL)
    body_for_scan = re.sub(r"--[^\n]*", " ", body_for_scan)
    body_for_scan = re.sub(r"'[^']*'", "''", body_for_scan)

    # Walk every DECLARE keyword and collect all @-vars up to the next
    # statement boundary. Handles multi-variable DECLAREs:
    #     DECLARE @x INT, @y INT = 1, @z VARCHAR(10) = 'foo'
    # and cursor / table declarations:
    #     DECLARE @cur CURSOR FOR ...
    #     DECLARE @t TABLE (id INT)
    stmt_boundary = re.compile(
        r"(?im)(?:\n\s*(?:SET|SELECT|INSERT|UPDATE|DELETE|MERGE|IF|ELSE|BEGIN|END|WHILE|EXEC(?:UTE)?|"
        r"PRINT|RETURN|WITH|CREATE|DROP|TRUNCATE|RAISERROR|THROW|TRY|CATCH|GOTO|USE|DECLARE|"
        r"FETCH|OPEN|CLOSE|DEALLOCATE|ALTER|GRANT|REVOKE)\b|;)"
    )
    for dm in re.finditer(r"(?i)\bDECLARE\b", body_for_scan):
        start = dm.end()
        boundary = stmt_boundary.search(body_for_scan, start)
        end = boundary.start() if boundary else len(body_for_scan)
        chunk = body_for_scan[start:end]
        for vm in re.finditer(r"(?<![@\w])@(\w+)", chunk):
            declared_names.add(vm.group(1).lower())

    # Collect references in a non-`= value` position. This deliberately
    # ignores @x in EXEC named-parameter syntax (EXEC proc @arg = 'foo'),
    # which would otherwise look like variable references. If a variable
    # is genuinely used in the body (arithmetic, comparison, SELECT, IF,
    # function arg, etc.) it will appear in at least one non-`=` position.
    referenced = set()
    eq_after = re.compile(r"\s*=")
    for m in re.finditer(r"(?<![@\w])@(\w+)", body_for_scan):
        # Skip if the @var is immediately followed by an '=' (possibly via
        # column-aligned spacing). This is ambiguous between 'SET @x = ...'
        # and EXEC named-parameter syntax; either way it isn't a clear
        # variable use that proves a missing DECLARE.
        if eq_after.match(body_for_scan, m.end()):
            continue
        referenced.add(m.group(1).lower())

    missing = sorted(r for r in referenced if r not in declared_names)
    if missing:
        result.warnings.append(
            f"Body references @variables with no matching DECLARE: "
            f"{', '.join('@' + m for m in missing)} "
            "(parser may have dropped a parameter, or these are declared via a pattern the scanner missed)"
        )
        result.needs_manual_review = True

    # Phase 8: assemble the batch
    header_lines = [
        f"-- Test batch generated from refactors/{proc_name}/{variant}.sql",
        "-- Generated by tools/build_test_batch.py",
        "-- DO NOT EDIT BY HAND; re-run the generator instead.",
        "",
        "SET NOCOUNT OFF;  -- harness needs row counts visible",
    ]
    if has_tvp:
        header_lines.append("-- This batch uses one or more table-valued parameters; populate them below.")
    header_lines.append("")

    out_chunks: list[str] = []
    out_chunks.append("\n".join(header_lines))

    if declare_lines:
        out_chunks.append("-- Parameter declarations")
        out_chunks.append("\n".join(declare_lines))
        out_chunks.append("")

    if tvp_seed_lines:
        out_chunks.append("-- TVP seed inserts (TODO: populate with realistic test values)")
        out_chunks.append("\n".join(tvp_seed_lines))
        out_chunks.append("")

    out_chunks.append("-- Procedure body (verbatim from DDL, with EXEC lsp_DbLogSqlEvent commented out)")
    out_chunks.append(body.rstrip())
    out_chunks.append("")

    return "\n".join(out_chunks), result


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------

def discover_proc_folders(include_deletes: bool, only: Optional[str]) -> list[Path]:
    folders = []
    for d in sorted(REFACTORS_DIR.iterdir()):
        if not d.is_dir():
            continue
        if d.name.startswith("00_"):
            continue
        if not include_deletes and DELETE_PROC_PATTERN.match(d.name):
            continue
        if only and d.name != only:
            continue
        folders.append(d)
    return folders


def process_folder(folder: Path) -> list[ConversionResult]:
    results: list[ConversionResult] = []
    for variant in ("Original", "Refactored"):
        src = folder / f"{variant}.sql"
        if not src.exists():
            r = ConversionResult(proc_name=folder.name, variant=variant)
            r.error = f"{variant}.sql not found"
            r.skipped = True
            results.append(r)
            continue

        sql_text = src.read_text(encoding="utf-8", errors="replace")
        batch_text, result = convert(sql_text, folder.name, variant)
        if not result.skipped and not result.error:
            dest = folder / f"{variant}.batch.sql"
            dest.write_text(batch_text, encoding="utf-8")
            result.output_path = dest
        results.append(result)
    return results


def write_report(all_results: list[ConversionResult]) -> None:
    by_proc: dict[str, list[ConversionResult]] = {}
    for r in all_results:
        by_proc.setdefault(r.proc_name, []).append(r)

    lines = [
        "# Batch conversion report",
        "",
        f"Generated by `tools/build_test_batch.py`. {len(by_proc)} procedure folders processed.",
        "",
        "Procs that need manual attention are flagged below. The batch files were still written, but",
        "you'll want to review them before pointing the harness at them.",
        "",
        "## Parser warnings",
        "",
    ]
    warned = [
        (p, rs) for p, rs in by_proc.items() if any(r.warnings for r in rs)
    ]
    if not warned:
        lines.append("_None._")
    else:
        for proc_name, rs in warned:
            lines.append(f"### {proc_name}")
            lines.append("")
            for r in rs:
                if not r.warnings:
                    continue
                lines.append(f"- **{r.variant}.batch.sql**")
                for w in r.warnings:
                    lines.append(f"  - {w}")
            lines.append("")

    # Param-count diff between Original and Refactored
    lines.append("## Original vs Refactored parameter count diffs")
    lines.append("")
    diffs = []
    for proc_name, rs in by_proc.items():
        by_v = {r.variant: r for r in rs}
        o = by_v.get("Original")
        r = by_v.get("Refactored")
        if o and r and len(o.parameters) != len(r.parameters):
            diffs.append((proc_name, len(o.parameters), len(r.parameters)))
    if not diffs:
        lines.append("_None — Original and Refactored parameter counts match for every proc._")
    else:
        lines.append("| Proc | Original params | Refactored params |")
        lines.append("|------|----------------:|------------------:|")
        for proc_name, oc, rc in diffs:
            lines.append(f"| {proc_name} | {oc} | {rc} |")
    lines.append("")

    lines.append("## Procedures needing manual review")
    lines.append("")

    needs_review = [
        p for p, rs in by_proc.items() if any(r.needs_manual_review for r in rs)
    ]
    if not needs_review:
        lines.append("_None._")
    else:
        for p in needs_review:
            lines.append(f"### {p}")
            lines.append("")
            for r in by_proc[p]:
                if not r.notes:
                    continue
                lines.append(f"- **{r.variant}.batch.sql**")
                for note in r.notes:
                    lines.append(f"  - {note}")
            lines.append("")

    lines.append("## Conversion errors")
    lines.append("")
    errs = [r for r in all_results if r.error]
    if not errs:
        lines.append("_None._")
    else:
        for r in errs:
            lines.append(f"- `{r.proc_name}` / `{r.variant}`: {r.error}")
    lines.append("")

    lines.append("## All converted procs")
    lines.append("")
    lines.append("| Proc | Original | Refactored | Params (O/R) | Notes |")
    lines.append("|------|---------:|-----------:|--------------|-------|")
    for proc_name in sorted(by_proc):
        rs = {r.variant: r for r in by_proc[proc_name]}
        orig = rs.get("Original")
        refac = rs.get("Refactored")

        def status(r: Optional[ConversionResult]) -> str:
            if not r:
                return "—"
            if r.error:
                return f"ERROR: {r.error}"
            return "ok"

        param_counts = (
            f"{len(orig.parameters) if orig else 0}/"
            f"{len(refac.parameters) if refac else 0}"
        )
        review_flag = ""
        if (orig and orig.needs_manual_review) or (refac and refac.needs_manual_review):
            review_flag = " (review)"
        lines.append(
            f"| {proc_name} | {status(orig)} | {status(refac)} | {param_counts} | {review_flag} |"
        )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--proc", help="Process only this single proc folder name", default=None)
    ap.add_argument(
        "--include-deletes",
        action="store_true",
        help="Include delete/purge procs (skipped by default).",
    )
    args = ap.parse_args(argv)

    folders = discover_proc_folders(args.include_deletes, args.proc)
    if not folders:
        print("No proc folders matched. Nothing to do.", file=sys.stderr)
        return 1

    all_results: list[ConversionResult] = []
    for folder in folders:
        rs = process_folder(folder)
        all_results.extend(rs)
        for r in rs:
            status = "OK"
            if r.error:
                status = f"ERROR ({r.error})"
            elif r.needs_manual_review:
                status = "OK (manual review)"
            print(f"  {r.proc_name:60s} {r.variant:11s} -> {status}")

    write_report(all_results)
    print(f"\nReport written to {REPORT_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
