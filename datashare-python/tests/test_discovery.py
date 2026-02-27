import pytest
from datashare_python.discovery import discover_activities, discover_workflows


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
