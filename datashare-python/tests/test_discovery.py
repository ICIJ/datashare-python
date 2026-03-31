import re
from importlib.metadata import EntryPoints
from unittest.mock import MagicMock

import datashare_python
import pytest
from _pytest.monkeypatch import MonkeyPatch
from datashare_python.discovery import (
    discover_activities,
    discover_dependencies,
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


@pytest.mark.parametrize("name", ["base", None])
def test_discover_dependencies(name: str | None) -> None:
    # When
    deps = discover_dependencies(name)
    # Then
    expected_deps = [
        "set_loggers",
        "set_event_loop",
        "set_es_client",
        "set_temporal_client",
    ]
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
        discover_dependencies(name=None)


def test_discover_dependencies_should_raise_for_multiple_entry_points(
    monkeypatch: MonkeyPatch,
) -> None:
    # Given
    def mocked_entry_points(name: str, group: str) -> EntryPoints:  # noqa: ARG001
        ep = MagicMock()
        ep.load.return_value = {"a": [], "b": []}
        entry_points = MagicMock()
        entry_points.__getitem__.return_value = ep
        return entry_points

    monkeypatch.setattr(datashare_python.discovery, "entry_points", mocked_entry_points)
    # When/Then
    expected = (
        'dependency registry contains multiples entries "a", "b",'
        " please select one by providing a name"
    )
    with pytest.raises(ValueError, match=re.escape(expected)):
        discover_dependencies(name=None)
