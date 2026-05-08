#!/usr/bin/env python3
"""
Export an Obsidian-style markdown vault to PDF, preserving relative
cross-document links between the resulting PDFs.

Usage:
    python3 export-to-pdf.py
    python3 export-to-pdf.py --vault /path/to/vault --output /path/to/pdf-export

Defaults (when run from inside pdf-export/):
    vault   = parent directory of this script (the masterclass vault)
    output  = the directory containing this script (pdf-export/)

Requirements:
    - pandoc           (brew install pandoc)
    - xelatex / TeX    (brew install --cask mactex     # large)
                  or:  (brew install --cask basictex   # small + tlmgr install of needed pkgs)
    - Python 3.9+

Behavior:
    - Originals are NEVER modified.
    - Markdown is staged in a temp dir, link-rewritten there, then converted.
    - Wikilink transforms:
          [[Page Name]]            -> [Page Name](Page%20Name.pdf)
          [[Page Name|alias]]      -> [alias](Page%20Name.pdf)
          [[Page#anchor]]          -> [Page](Page.pdf#anchor)
    - Filename lookup is case-insensitive against the actual .md files.
"""

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
from pathlib import Path


WIKILINK_RE = re.compile(
    r"\[\[([^\]|#]+?)(?:#([^\]|]+?))?(?:\|([^\]]+?))?\]\]"
)


def build_filename_index(vault: Path) -> dict:
    return {md.stem.lower(): md.stem for md in vault.glob("*.md")}


def rewrite_wikilinks(text: str, index: dict, src_name: str) -> tuple:
    warnings = []

    def repl(match):
        page = match.group(1).strip()
        anchor = (match.group(2) or "").strip()
        alias = (match.group(3) or "").strip()

        actual = index.get(page.lower())
        if actual is None:
            warnings.append(f"  [{src_name}] unresolved wikilink: [[{match.group(0)[2:-2]}]]")
            return alias if alias else page

        url = urllib.parse.quote(actual + ".pdf")
        if anchor:
            url += "#" + urllib.parse.quote(anchor)
        display = alias if alias else page
        return f"[{display}]({url})"

    return WIKILINK_RE.sub(repl, text), warnings


def stage_markdown(vault: Path, staging: Path, index: dict) -> list:
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    staged = []
    all_warnings = []
    for src in sorted(vault.glob("*.md")):
        rewritten, warns = rewrite_wikilinks(src.read_text(encoding="utf-8"), index, src.name)
        all_warnings.extend(warns)
        dst = staging / src.name
        dst.write_text(rewritten, encoding="utf-8")
        staged.append(dst)

    if all_warnings:
        print("Warnings:")
        for w in all_warnings:
            print(w)
    else:
        print(f"Staged {len(staged)} note(s); all wikilinks resolved.")

    return staged


def check_tools() -> None:
    missing = []
    for tool in ("pandoc", "xelatex"):
        if shutil.which(tool) is None:
            missing.append(tool)
    if missing:
        print("Missing required tool(s): " + ", ".join(missing), file=sys.stderr)
        print("Install hints:", file=sys.stderr)
        print("  brew install pandoc", file=sys.stderr)
        print("  brew install --cask basictex   # then: sudo tlmgr update --self && sudo tlmgr install ...", file=sys.stderr)
        print("  (or) brew install --cask mactex", file=sys.stderr)
        sys.exit(1)


def convert_to_pdf(staged: list, output: Path) -> list:
    output.mkdir(parents=True, exist_ok=True)

    common_args = [
        "pandoc",
        "--from=markdown+pipe_tables+backtick_code_blocks",
        "--pdf-engine=xelatex",
        "-V", "mainfont=Helvetica Neue",
        "-V", "monofont=Menlo",
        "-V", "geometry:margin=1in",
        "-V", "colorlinks=true",
        "-V", "linkcolor=NavyBlue",
        "-V", "urlcolor=NavyBlue",
        "-V", "toccolor=NavyBlue",
        "--highlight-style=tango",
    ]

    produced = []
    failures = []
    for src in staged:
        pdf_path = output / (src.stem + ".pdf")
        result = subprocess.run(common_args + ["-o", str(pdf_path), str(src)],
                                capture_output=True, text=True)
        if result.returncode != 0:
            failures.append((src.name, result.stderr.strip()))
            print(f"  FAIL  {src.name}")
            print(result.stderr)
        else:
            produced.append(pdf_path)
            print(f"  ok    {src.name} -> {pdf_path.name}")

    if failures:
        print(f"\n{len(failures)} file(s) failed:")
        for name, err in failures:
            last = err.splitlines()[-1] if err else "unknown error"
            print(f"  - {name}: {last}")

    return produced


def main() -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Export Obsidian vault to linked PDFs.")
    parser.add_argument("--vault", type=Path, default=here.parent,
                        help="Vault directory (default: parent of this script)")
    parser.add_argument("--output", type=Path, default=here,
                        help="Output directory for PDFs (default: directory of this script)")
    args = parser.parse_args()

    vault = args.vault.resolve()
    output = args.output.resolve()

    if not vault.exists():
        print(f"Vault not found: {vault}", file=sys.stderr)
        return 1

    check_tools()

    print(f"Vault:  {vault}")
    print(f"Output: {output}\n")

    index = build_filename_index(vault)
    print(f"Indexed {len(index)} note(s).")

    with tempfile.TemporaryDirectory(prefix="md-staging-") as tmp:
        staging = Path(tmp)
        staged = stage_markdown(vault, staging, index)
        print()
        print("Converting to PDF...")
        produced = convert_to_pdf(staged, output)

    print(f"\nProduced {len(produced)} PDF(s) in {output}")
    return 0 if len(produced) == len(index) else 2


if __name__ == "__main__":
    sys.exit(main())
