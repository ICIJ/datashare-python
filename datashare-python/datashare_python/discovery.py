import re
from collections.abc import Callable, Iterable
from importlib.metadata import entry_points

from .utils import ActivityWithProgress

Activity = ActivityWithProgress | Callable | type

_WORKFLOW_GROUPS = "datashare.workflows"
_ACTIVITIES_GROUPS = "datashare.activities"


def discover_workflows(names: list[str]) -> Iterable[tuple[str, type]]:
    pattern = None if not names else re.compile(rf"^{'|'.join(names)}$")
    impls = entry_points(group=_WORKFLOW_GROUPS)
    for wf_impls in impls:
        wf_impls = wf_impls.load()  # noqa: PLW2901
        if not isinstance(wf_impls, list | tuple | set):
            wf_impls = [wf_impls]  # noqa: PLW2901
        for wf_impl in wf_impls:
            wf_name = _parse_wf_name(wf_impl)
            if pattern and not pattern.match(wf_name):
                continue
            yield wf_name, wf_impl


def discover_activities(names: list[str]) -> Iterable[tuple[str, Activity]]:
    pattern = None if not names else re.compile(rf"^{'|'.join(names)}$")
    impls = entry_points(group=_ACTIVITIES_GROUPS)
    for act_impls in impls:
        act_impls = act_impls.load()  # noqa: PLW2901
        if not isinstance(act_impls, list | tuple | set):
            act_impls = [act_impls]  # noqa: PLW2901
        for act_impl in act_impls:
            act_name = _parse_activity_name(act_impl)
            if pattern and not pattern.match(act_name):
                continue
            yield act_name, act_impl


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
