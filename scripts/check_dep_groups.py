#!/usr/bin/env python3
"""Guard the root `[dependency-groups]` against drift from the member extras.

The uv workspace root (`pyproject.toml`) is not a distributable package; its
`[dependency-groups] test` exists only so `uv.lock` covers everything the test
suites need. It mirrors, by hand, the union of `py/pyproject.toml` and
`json/pyproject.toml`'s `[project.optional-dependencies] test`.

`uv lock --check` cannot catch a divergence between the two -- the lock is
consistent either way -- so the failure mode is silent: a dependency gets added
to a package extra, the root group is not updated, and CI (which installs the
root group) runs without it. That has already happened once, with `hypothesis`.

This asserts the root group is a *superset* of both extras at the requirement
name level. Version specifiers are deliberately not compared: the root is
allowed to pin a tighter floor than a published extra, which is a legitimate
"the dev environment wants something newer" choice.

Run with no arguments from the repository root; exits non-zero on drift.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (member pyproject, extra name, root group name)
CHECKS = [
    ("py/pyproject.toml", "test", "test"),
    ("json/pyproject.toml", "test", "test"),
    ("py/pyproject.toml", "bench", "bench"),
    ("json/pyproject.toml", "bench", "bench"),
]


def load(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def requirement_name(requirement: str) -> str:
    """The PEP 508 distribution name of a requirement, normalised.

    Enough of a parser for this repo's requirements: strip the extras bracket,
    any version/marker tail, and apply PEP 503 normalisation (`testing.postgresql`
    and `testing-postgresql` are the same project).
    """
    name = requirement.strip()
    for separator in ("[", ";", "=", "<", ">", "!", "~", " ", "@"):
        name = name.split(separator, 1)[0]
    return name.strip().lower().replace("_", "-").replace(".", "-")


def main() -> int:
    root = load(ROOT / "pyproject.toml")
    groups = root.get("dependency-groups", {})

    failures: list[str] = []
    for member_path, extra, group in CHECKS:
        member = load(ROOT / member_path)
        extras = member.get("project", {}).get("optional-dependencies", {})
        if extra not in extras:
            continue
        required = {requirement_name(r) for r in extras[extra]}
        if group not in groups:
            failures.append(
                f"{member_path} has a [{extra}] extra but the root pyproject.toml "
                f"has no [dependency-groups] {group}"
            )
            continue
        have = {requirement_name(r) for r in groups[group]}
        missing = sorted(required - have)
        if missing:
            failures.append(
                f"[dependency-groups] {group} is missing "
                f"{', '.join(missing)} (required by {member_path} [{extra}])"
            )

    if failures:
        print("Root dependency groups have drifted from the member extras:\n")
        for failure in failures:
            print(f"  - {failure}")
        print(
            "\nAdd the missing requirements to [dependency-groups] in the root "
            "pyproject.toml and re-run `uv lock`."
        )
        return 1

    print("Root [dependency-groups] cover every member extra.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
