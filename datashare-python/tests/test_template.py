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
    pyproject_toml = tomlkit.loads(pyproject_toml_path.read_text())

    project = pyproject_toml["project"]
    assert not project["authors"]

    dependencies = project["dependencies"]
    assert any(d.startswith("datashare-python") for d in dependencies)
    assert any(d.startswith("icij-common") for d in dependencies)
    assert any(d.startswith("temporalio") for d in dependencies)

    assert "uv" not in pyproject_toml["tool"]

    entry_points = project["entry-points"]
    wf_entrypoints = entry_points["datashare.workflows"]["workflows"]
    assert wf_entrypoints == "test_project.workflows:WORKFLOWS"
    acts = entry_points["datashare.activities"]["activities"]
    assert acts == "test_project.activities:ACTIVITIES"
