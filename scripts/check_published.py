#!/usr/bin/env python3
"""Decide whether the version in a manifest still needs publishing.

Used by the `detect-release` jobs in `.github/workflows/{rust,python-package}.yaml`
to keep the release jobs from running at all when the version in the tree has
already been published (see #56). `cargo publish` and `twine upload
--skip-existing` both tolerate a re-run, so this is about not spending CI
minutes -- which is exactly why a registry hiccup must never fail the build.

Failure policy: anything other than a definitive "this version is not on the
registry" answer (network error, 5xx, unparseable body) resolves to
``should_release=false`` with a warning annotation. Skipping a publish is
recoverable -- push an empty commit, or publish by hand -- while a red build on
`main` for an unrelated registry outage is not.

Usage:
    check_published.py --registry {crates,pypi} --package NAME --manifest PATH
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tomllib
import urllib.error
import urllib.request

# Identifies the client to crates.io, which rejects requests without a
# User-Agent outright (403 + "please set a user agent").
USER_AGENT = "pgpq-release-check (+https://github.com/adriangb/pgpq)"
TIMEOUT_SECONDS = 30


def log(message: str) -> None:
    print(message, flush=True)


def warn(message: str) -> None:
    # `::warning::` surfaces in the job summary without failing the step.
    print(f"::warning::{message}", flush=True)


def read_version(manifest: str) -> str:
    """Read the version from a Cargo.toml `[package]` or pyproject.toml `[project]`."""
    with open(manifest, "rb") as f:
        data = tomllib.load(f)
    for table in ("package", "project"):
        version = data.get(table, {}).get("version")
        if isinstance(version, str):
            return version
    raise SystemExit(f"no [package]/[project] version found in {manifest}")


def fetch_json(url: str) -> dict | None:
    """GET a JSON document. Returns ``None`` on 404, raises on anything else."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def published_versions(registry: str, package: str) -> set[str]:
    """Every version the registry knows about, including yanked ones.

    Yanked releases still occupy their version number, so for the purpose of
    "can this be published?" they count as published.
    """
    if registry == "crates":
        payload = fetch_json(f"https://crates.io/api/v1/crates/{package}")
        if payload is None:
            return set()
        return {v["num"] for v in payload["versions"]}
    payload = fetch_json(f"https://pypi.org/pypi/{package}/json")
    if payload is None:
        return set()
    return set(payload["releases"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", choices=("crates", "pypi"), required=True)
    parser.add_argument("--package", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()

    version = read_version(args.manifest)
    log(f"{args.package} {version} (from {args.manifest})")

    try:
        existing = published_versions(args.registry, args.package)
    # Deliberately broad: every failure mode here (DNS, TLS, 5xx, malformed
    # body) has the same conservative answer.
    except Exception as exc:
        warn(
            f"could not reach the {args.registry} registry ({exc!r}); "
            f"assuming {args.package} {version} is already published and "
            f"skipping the release job. Re-run this workflow to retry."
        )
        should_release = False
    else:
        if not existing:
            log(f"{args.package} is not on the {args.registry} registry yet")
            should_release = True
        elif version in existing:
            log(
                f"{args.package} {version} is already on the {args.registry} "
                f"registry; skipping the release job"
            )
            should_release = False
        else:
            log(
                f"{args.package} {version} is not on the {args.registry} "
                f"registry ({len(existing)} versions known); releasing"
            )
            should_release = True

    value = "true" if should_release else "false"
    log(f"should_release={value}")
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"should_release={value}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
