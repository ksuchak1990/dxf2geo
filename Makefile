clean-code:
	ruff check --select I --fix
	ruff format
	ruff check --fix
