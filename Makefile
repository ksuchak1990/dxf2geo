clean-code:
	ruff check --select I --fix
	ruff format
	ruff check --fix

test:
	pytest .

docs:
	pdoc ./src/dxf2geo --docformat numpy
