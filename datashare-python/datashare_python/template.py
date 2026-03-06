import os
import tarfile
from copy import deepcopy
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

import tomlkit
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

PACKAGE_DIR = Path(__file__).parent
PACKAGE_ROOT = PACKAGE_DIR.parent

ALLOWED_EXTS = {
    ".py",
    ".md",
    ".python-version",
    ".lock",
    ".toml",
}


class CopyTemplateHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:  # noqa: ARG002
        # Only generate the worker template when building the sources,
        # the wheel is then generated from this first build
        if self.target_name == "sdist":
            build_template_tarball()
        build_data["artifacts"].append("datashare_python/worker-template.tar.gz")


def build_template_tarball() -> None:
    template_dir = PACKAGE_ROOT.parent.joinpath("worker-template")
    tar_path = PACKAGE_DIR.joinpath("worker-template.tar.gz")
    if tar_path.exists():
        os.remove(tar_path)

    with tarfile.open(tar_path, "w:gz") as tar:
        for path in template_dir.rglob("*"):  # Skip hidden files
            is_hidden = path.name.startswith(".") or any(
                "." in p for p in path.parts[:-1]
            )
            if is_hidden or not path.is_file() or path.suffix not in ALLOWED_EXTS:
                continue
            tar.add(path, arcname=path.relative_to(template_dir))


def init_project(name: str, path: Path) -> None:
    destination = path / name
    template_tar = files("datashare_python")
    with (
        as_file(template_tar / "worker-template.tar.gz") as tar_path,
        tarfile.open(tar_path, mode="r:gz") as tar,
    ):
        tar.extractall(destination)
    pyproject_toml_path = destination / "pyproject.toml"
    pyproject_toml = tomlkit.loads(pyproject_toml_path.read_text())
    pyproject_toml = _update_pyproject_toml(pyproject_toml, project_name=name)
    pyproject_toml_path.write_text(tomlkit.dumps(pyproject_toml))


_BASE_DEPS = {"datashare-python", "icij-common", "temporalio"}


def _update_pyproject_toml(
    pyproject_toml: dict[str, Any], *, project_name: str
) -> dict[str, Any]:
    lower_snaked = project_name.replace("-", "_").lower()

    pyproject_toml = deepcopy(pyproject_toml)

    pyproject_toml["tool"]["uv"].pop("sources")
    pyproject_toml["tool"]["uv"].pop("index")

    project = pyproject_toml["project"]
    project["authors"] = []
    project["urls"] = []
    project["dependencies"] = sorted(
        d
        for d in project["dependencies"]
        if any(d.startswith(base) for base in _BASE_DEPS)
    )
    project["dependencies"] = sorted(
        d
        for d in project["dependencies"]
        if any(d.startswith(base) for base in _BASE_DEPS)
    )
    project.pop("optional-dependencies")

    entry_points = project["entry-points"]

    wf_entry_point = entry_points["datashare.workflows"]["workflows"]
    wf_entry_point = wf_entry_point.replace("worker_template", lower_snaked)
    entry_points["datashare.workflows"]["workflows"] = wf_entry_point

    activities_entry_point = entry_points["datashare.activities"]["activities"]
    activities_entry_point = activities_entry_point.replace(
        "worker_template", lower_snaked
    )
    entry_points["datashare.activities"]["activities"] = activities_entry_point

    return pyproject_toml
