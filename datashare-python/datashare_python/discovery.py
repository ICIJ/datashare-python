import logging
import re
from collections.abc import Callable, Iterable
from importlib.metadata import entry_points

from .types_ import ContextManagerFactory
from .utils import ActivityWithProgress

logger = logging.getLogger(__name__)

Activity = ActivityWithProgress | Callable | type

_DEPENDENCIES = "dependencies"
_WORKFLOW_GROUPS = "datashare.workflows"
_ACTIVITIES_GROUPS = "datashare.activities"
_DEPENDENCIES_GROUPS = "datashare.dependencies"

_RegisteredWorkflow = tuple[str, type]
_RegisteredActivity = tuple[str, Activity]
_Dependencies = list[ContextManagerFactory]
_Discovery = tuple[
    Iterable[_RegisteredWorkflow] | None,
    Iterable[_RegisteredActivity] | None,
    _Dependencies | None,
]


def discover(
    wf_names: list[str] | None, *, act_names: list[str] | None, deps_name: str | None
) -> _Discovery:
    discovered = ""
    wfs = None
    if wf_names is not None:
        discovered_wfs = discover_workflows(wf_names)
        if discovered_wfs:
            wf_names, wfs = zip(*discovered_wfs, strict=True)
            if wf_names:
                n_wfs = len(wf_names)
                discovered += (
                    f"- {n_wfs} workflow{'s' if n_wfs > 1 else ''}:"
                    f" {', '.join(wf_names)}"
                )
    acts = None
    if act_names is not None:
        discovered_acts = discover_activities(act_names)
        if discovered_acts:
            act_names, acts = zip(*discovered_acts, strict=True)
            if act_names:
                if discovered:
                    discovered += "\n"
                n_acts = len(act_names)
                discovered += (
                    f"- {n_acts} activit{'ies' if n_acts > 1 else 'y'}:"
                    f" {', '.join(act_names)}"
                )
    if not acts and not wfs:
        msg = "Couldn't find any registered activity nor workflow matching "
        if wf_names:
            msg += "workflow patterns " + ", ".join(wf_names) + " "
        if act_names:
            msg += "activity patterns " + ", ".join(act_names)
        raise ValueError(msg)
    deps = None
    if deps_name is not None:
        deps = discover_dependencies(deps_name)
    if deps:
        n_deps = len(deps)
        discovered += "\n"
        deps_names = (d.__name__ for d in deps)
        discovered += (
            f"- {n_deps} dependenc{'ies' if n_deps > 1 else 'y'}:"
            f" {', '.join(deps_names)}"
        )
    logger.info("discovered:\n%s", discovered)
    return wfs, acts, deps


def discover_workflows(names: list[str]) -> list[_RegisteredWorkflow]:
    pattern = None if not names else re.compile(rf"^{'|'.join(names)}$")
    impls = entry_points(group=_WORKFLOW_GROUPS)
    registered = []
    for wf_impls in impls:
        wf_impls = wf_impls.load()  # noqa: PLW2901
        if not isinstance(wf_impls, list | tuple | set):
            wf_impls = [wf_impls]  # noqa: PLW2901
        for wf_impl in wf_impls:
            wf_name = _parse_wf_name(wf_impl)
            if pattern and not pattern.match(wf_name):
                continue
            registered.append((wf_name, wf_impl))
    return registered


def discover_activities(names: list[str]) -> list[_RegisteredActivity]:
    pattern = None if not names else re.compile(rf"^{'|'.join(names)}$")
    impls = entry_points(group=_ACTIVITIES_GROUPS)
    registered = []
    for act_impls in impls:
        act_impls = act_impls.load()  # noqa: PLW2901
        if not isinstance(act_impls, list | tuple | set):
            act_impls = [act_impls]  # noqa: PLW2901
        for act_impl in act_impls:
            act_name = _parse_activity_name(act_impl)
            if pattern and not pattern.match(act_name):
                continue
            registered.append((act_name, act_impl))
    return registered


def discover_dependencies(name: str) -> _Dependencies:
    impls = entry_points(name=_DEPENDENCIES, group=_DEPENDENCIES_GROUPS)
    if not impls:
        available_impls = entry_points(group=_DEPENDENCIES_GROUPS)
        msg = (
            f'failed to find dependency: "{name}", '
            f"available dependencies: {available_impls}"
        )
        raise LookupError(msg)
    if len(impls) > 1:
        msg = f'found multiple dependencies for name "{name}": {impls}'
        raise ValueError(msg)
    deps_registry = impls[_DEPENDENCIES].load()
    try:
        return deps_registry[name]
    except KeyError as e:
        available = list(deps_registry)
        msg = (
            f'failed to find dependency for name "{name}", available dependencies: '
            f"{available}"
        )
        raise LookupError(msg) from e


def _parse_wf_name(wf_type: type) -> str:
    if not isinstance(wf_type, type):
        msg = (
            f"expected registered workflow implementation to be a temporal workflow"
            f" decorated with @workflow.defn(name=<name>) class, found: {type(wf_type)}"
        )
        raise TypeError(msg)

    wf_defn = getattr(wf_type, "__temporal_workflow_definition", None)
    if wf_defn is None:
        msg = (
            f"expected registered workflow implementation to be a temporal workflow"
            f" decorated with @workflow.defn(name=<name>) class, found: {wf_type}"
        )
        raise ValueError(msg)
    if wf_defn.name is None:
        msg = (
            "missing workflow definition name, please register your workflow"
            " with an explicit name: @workflow.defn(name=<name>)"
        )
        raise ValueError(msg)
    return wf_defn.name


def _parse_activity_name(act: Activity) -> str:
    act_defn = getattr(act, "__temporal_activity_definition", None)
    if act_defn is None:
        msg = (
            f"expected registered actitiby implementation to be a temporal activity"
            f" decorated with @activity.defn(name=<name>), found: {act}"
        )
        raise ValueError(msg)
    if act_defn.name is None:
        msg = (
            "missing activity definition name, please register your activities"
            " with an explicit name: @activity.defn(name=<name>)"
        )
        raise ValueError(msg)
    return act_defn.name
