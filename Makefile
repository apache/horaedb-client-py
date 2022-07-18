SHELL = /bin/bash

install-deps:
	pip install maturin

build:
	maturin build --release

fmt:
	cargo fmt -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings
