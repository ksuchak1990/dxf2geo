.DEFAULT_GOAL := pre-commit

.PHONY: pre-commit fix lint check test docs

pre-commit: fix

fix:
	ruff check --fix .
	ruff format .

lint:
	ruff format --check .
	ruff check .

check: lint

test:
	pytest .

docs:
	pdoc ./src/dxf2geo --docformat numpy
