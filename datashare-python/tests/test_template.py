from pathlib import Path

import tomlkit
from datashare_python.template import init_project


async def test_init_project(
    local_template_build,  # noqa: ANN001, ARG001
    tmp_path: Path,
) -> None:
    # Given
    test_worker_project = "test-project"

    # When
    init_project(test_worker_project, tmp_path)

    # Then
    project_path = tmp_path / test_worker_project

    assert project_path.exists()
    pyproject_toml_path = project_path / "pyproject.toml"
    assert pyproject_toml_path.exists()
    package_path = project_path / "test_project"
    assert package_path.exists()

    pyproject_toml = tomlkit.loads(pyproject_toml_path.read_text())

    project = pyproject_toml["project"]
    assert not project["authors"]

    name = project["name"]
    assert name == "test-project"
    version = project["version"]
    assert version == "0.1.0"
    dependencies = project["dependencies"]
    assert any(d.startswith("datashare-python") for d in dependencies)
    assert any(d.startswith("temporalio") for d in dependencies)

    assert "optional-dependencies" not in project

    assert "uv" not in pyproject_toml["tool"]

    entry_points = project["entry-points"]
    wf_entrypoints = entry_points["datashare.workflows"]["workflows"]
    assert wf_entrypoints == "test_project.workflows:WORKFLOWS"
    acts_entrypoints = entry_points["datashare.activities"]["activities"]
    assert acts_entrypoints == "test_project.activities:ACTIVITIES"
    deps_entrypoints = entry_points["datashare.dependencies"]["dependencies"]
    assert deps_entrypoints == "test_project.dependencies:DEPENDENCIES"
    cfg_entrypoints = entry_points["datashare.worker_config_cls"]["worker_config_cls"]
    assert cfg_entrypoints == "test_project.config_:WORKER_CONFIG_CLS"

    hatch_sdist = pyproject_toml["tool"]["hatch"]["build"]["targets"]["wheel"]
    assert hatch_sdist["packages"] == ["test_project"]

    build_system = pyproject_toml["build-system"]
    assert build_system["package"] == "test_project"
