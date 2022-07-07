SHELL = /bin/bash

install-deps:
	pip install maturin

build:
	maturin build --release
