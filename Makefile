PHONY: init build-develop build-cli test

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
	. ./.venv/bin/activate && maturin develop -m json/Cargo.toml
	cargo build --package pgpq-cli

test: build-develop
	cargo test
	./.venv/bin/python -m pytest

lint: build-develop
	./.venv/bin/pre-commit run --all-files

build-cli:
	cargo build --package pgpq-cli --release

install-cli: build-cli
	cp target/release/pgpq /usr/local/bin/
