import json
from collections.abc import Iterable
from pathlib import Path


def read_jsonl(path: Path) -> Iterable[dict]:
    with path.open() as f:
        for line in f:
            line = line.strip()  # noqa: PLW2901
            if line:
                yield json.loads(line)
