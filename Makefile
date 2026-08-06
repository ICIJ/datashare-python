lock-all:
	./scripts/lock-dist.sh worker-template ${uv_extra}
	./scripts/lock-dist.sh asr-worker ${uv_extra}
	./scripts/lock-dist.sh extract-worker ${uv_extra}
	./scripts/lock-dist.sh passport-worker ${uv_extra}
	./scripts/lock-dist.sh translation-worker ${uv_extra}
	./scripts/lock-dist.sh workflows-worker ${uv_extra}

lock-dist:
	./scripts/lock-dist.sh ${project} ${uv_extra}

create-venv:
	[ -d .venv ] || uv venv --python 3.13

install-deps:
	uv pip install --override requirements_overrides.txt -e asr_worker/
	uv pip install -e translation-worker/
	uv pip install -e datashare-python/
	uv pip install -e worker-template/
	uv pip install -r requirements_dev.txt

create-dirs:
	mkdir .data .data/temporal .data/datashare || true
	ln -s resources/files/asr .data/temporal/asr || true

install:
	make create-venv
	make install-deps
	make create-dirs
	pre-commit install