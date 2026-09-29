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

create-dirs:
	mkdir .data .data/temporal .data/datashare || true
	ln -s resources/files/asr .data/temporal/asr || true

install:
	make create-venv
	make install-deps
	make create-dirs
	pre-commit install