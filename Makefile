create-venv:
	[ -d .venv ] || uv venv --python 3.10

install-deps:
	uv pip install --override requirements_overrides.txt -e asr_worker/
	uv pip install --override requirements_overrides.txt -e datashare_cli/
	uv pip install -r requirements_dev.txt

create-dirs:
	mkdir .data .data/temporal .data/datashare || true
	ln -s resources/files/asr .data/temporal/asr || true

install:
	make create-venv
	make install-deps
	make create-dirs
	pre-commit install