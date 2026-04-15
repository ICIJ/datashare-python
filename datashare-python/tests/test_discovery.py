import re
from importlib.metadata import EntryPoints
from unittest.mock import MagicMock

import datashare_python
import pytest
from _pytest.monkeypatch import MonkeyPatch
from datashare_python.discovery import (
    discover_activities,
    discover_dependencies,
    discover_worker_configs,
    discover_workflows,
)


@pytest.mark.parametrize(
    ("names", "expected_workflows"),
    [
        ([], {"ping", "translate-and-classify"}),
        (["ping", "translate-and-classify"], {"ping", "translate-and-classify"}),
        (["ping"], {"ping"}),
        (["pi.*"], {"ping"}),
        (["pong"], set()),
    ],
)
def test_discover_workflows(names: list[str], expected_workflows: set[str]) -> None:
    # When
    workflows = {wf_name for wf_name, _ in discover_workflows(names)}
    # Then
    assert workflows == expected_workflows


@pytest.mark.parametrize(
    ("names", "expected_activities"),
    [
        (
            [],
            {
                "classify-docs",
                "create-classification-batches",
                "create-translation-batches",
                "pong",
                "translate-docs",
            },
        ),
        (["translate-docs"], {"translate-docs"}),
        ([".*transl.*"], {"create-translation-batches", "translate-docs"}),
        (["idontexist"], set()),
    ],
)
def test_discover_activities(names: list[str], expected_activities: set[str]) -> None:
    # When
    activities = {act_name for act_name, _ in discover_activities(names)}
    # Then
    assert activities == expected_activities


def test_discover_dependencies() -> None:
    # When
    name = "base"
    deps = discover_dependencies(name)
    # Then
    expected_deps = ["set_worker_config", "set_loggers", "set_es_client"]
    assert [d.__name__ for d in deps] == expected_deps


def test_discover_dependencies_should_raise_for_unknown_dep() -> None:
    # Given
    unknown_dep = "unknown_dep"
    # When/Then
    expected = (
        'failed to find dependency for name "unknown_dep", '
        "available dependencies: ['base']"
    )
    with pytest.raises(LookupError, match=re.escape(expected)):
        discover_dependencies(unknown_dep)


def test_discover_dependencies_should_raise_for_conflicting_deps(
    monkeypatch: MonkeyPatch,
) -> None:
    # Given
    def mocked_entry_points(name: str, group: str) -> EntryPoints:  # noqa: ARG001
        entry_points = MagicMock()
        entry_points.__len__.return_value = 2
        return entry_points

    monkeypatch.setattr(datashare_python.discovery, "entry_points", mocked_entry_points)
    # When/Then
    expected = "found multiple dependencies for name"
    with pytest.raises(ValueError, match=re.escape(expected)):
        discover_dependencies(name="some_name")


def test_discover_worker_configs() -> None:
    # Given
    name = "base"
    # When
    worker_config_cls = discover_worker_configs(name)
    # Then
    assert worker_config_cls.__name__ == "TranslateAndClassifyWorkerConfig"


def test_discover_worker_configs_should_raise_for_unknown_dep() -> None:
    # Given
    unknown_worker_config = "unknown_worker_config"
    # When/Then
    expected = (
        'failed to find worker config for name "unknown_worker_config", '
        "available worker configs: ['base']"
    )
    with pytest.raises(LookupError, match=re.escape(expected)):
        discover_worker_configs(unknown_worker_config)


def test_discover_worker_configs_should_raise_for_conflicting_deps(
    monkeypatch: MonkeyPatch,
) -> None:
    # Given
    def mocked_entry_points(name: str, group: str) -> EntryPoints:  # noqa: ARG001
        entry_points = MagicMock()
        entry_points.__len__.return_value = 2
        return entry_points

    monkeypatch.setattr(datashare_python.discovery, "entry_points", mocked_entry_points)
    # When/Then
    expected = "found multiple worker configs for name"
    with pytest.raises(ValueError, match=re.escape(expected)):
        discover_worker_configs(name="some_name")
