PHONY: init build test

.init:
	rm -rf .venv
	uv venv
	uv pip install -e ./py[test,bench]
	uv run pre-commit install
	touch .init

.clean:
	rm -rf .init

init: .clean .init

build-develop: .init 
	uvx maturin develop -m py/Cargo.toml --strip
	uvx maturin develop -m json/Cargo.toml --strip

test: build-develop
	cargo test
	uv run pytest

lint: build-develop
	uv run pre-commit run --all-files
