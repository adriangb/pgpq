PHONY: init build test

.init:
	rm -rf .venv
	python -m venv .venv
	./.venv/bin/pip install -e ./py[test,bench]
	./.venv/bin/pre-commit install
	touch .init

.clean:
	rm -rf .init

init: .clean .init

build-develop: .init
	. ./.venv/bin/activate && maturin develop -m py/Cargo.toml

test: build-develop
	cargo test
	./.venv/bin/python -m pytest

lint: build-develop
	./.venv/bin/pre-commit run --all-files
