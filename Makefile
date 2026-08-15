.PHONY: init build-develop test lint

# `uv sync --locked` fails rather than silently re-resolving if `uv.lock` is out
# of date. If you changed a dependency in one of the pyproject.toml files, run
# `uv lock` (or `uv lock --upgrade` to refresh pins) and commit the result.
#
# `--no-install-workspace` keeps uv from building pgpq/arrow-json: those are
# installed into the same virtualenv by `maturin develop` below, which is what
# we actually want to test against.
.init: pyproject.toml uv.lock py/pyproject.toml json/pyproject.toml
	uv sync --locked --group test --group bench --no-install-workspace
	uv run --no-sync pre-commit install
	touch .init

.clean:
	rm -rf .init

init: .clean .init

# `--no-sync` on every `uv run`: the maturin-built extension modules are not
# part of the lock, so letting uv re-sync would prune them from the venv.
build-develop: .init
	uv run --no-sync maturin develop -m py/Cargo.toml --strip
	uv run --no-sync maturin develop -m json/Cargo.toml --strip

test: build-develop
	cargo test
	uv run --no-sync pytest

# `uv lock --check` only proves the lock matches the manifests; it cannot see that
# the root [dependency-groups] have drifted from the member extras, hence
# check_dep_groups.py (also run as its own step in CI).
lint: build-develop
	uv lock --check
	python3 scripts/check_dep_groups.py
	uv run --no-sync pre-commit run --all-files
