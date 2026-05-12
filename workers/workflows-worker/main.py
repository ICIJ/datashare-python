import json
import sys
from dataclasses import asdict, dataclass
from enum import StrEnum
from importlib.metadata import version
from typing import Any

from packaging.version import Version

_1_0_0 = Version("1.0.0")


class _VersionEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Version):
            return str(o)
        return super().default(o)


class BumpType(StrEnum):
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"


@dataclass(frozen=True)
class Bumped:
    name: str
    current: Version
    next: Version

    @property
    def is_breaking(self) -> bool:
        current_release = self.current.release
        next_release = self.next.release
        if self.current < _1_0_0:
            if self.next >= _1_0_0:
                return True
            return current_release[1] != next_release[1]
        return current_release[0] != next_release[0]


@dataclass(frozen=True)
class BumpInfo:
    current: Version
    next: Version
    bump_type: BumpType
    bumped: Bumped

    current_version = ""


def _bump_version(current: Version, *, breaking: bool) -> tuple[Version, BumpType]:
    release = current.release
    major = release[0]
    minor = release[1]
    patch = release[2]
    if current < _1_0_0:
        if breaking:
            return Version(f"0.{minor + 1}.{patch}"), BumpType.MINOR
        return Version(f"0.{minor}.{patch + 1}"), BumpType.PATCH
    if breaking:
        return Version(f"{major + 1}.{minor}.{patch}"), BumpType.MAJOR
    return Version(f"{major}.{minor + 1}.{patch}"), BumpType.MINOR


def _validate_version(current: Version) -> None:
    try:
        _, _, _ = current.release
    except TypeError as e:
        msg = f"Invalid version: {e}, expected version matching major.minor.patch"
        raise ValueError(msg) from e


def _get_bump_info(worker: str, next_worker_version: Version) -> BumpInfo:
    _validate_version(next_worker_version)
    current = Version(version("datashare-workflows-worker"))
    _validate_version(current)
    bumped_current = Version(version(worker))
    _validate_version(bumped_current)

    bumped = Bumped(name=worker, next=next_worker_version, current=bumped_current)
    next_workflow_version, bump_type = _bump_version(
        current, breaking=bumped.is_breaking
    )
    return BumpInfo(
        current=current, next=next_workflow_version, bumped=bumped, bump_type=bump_type
    )


def main() -> None:
    worker_app, worker_version = sys.argv[1:]
    worker_version = Version(worker_version)
    info = _get_bump_info(worker_app, worker_version)
    print(json.dumps(asdict(info), cls=_VersionEncoder))


if __name__ == "__main__":
    main()
