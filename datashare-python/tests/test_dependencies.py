from typing import Any
from unittest.mock import MagicMock

import pytest
from datashare_python.dependencies import (
    add_missing_args,
    component_teardown,
    lifespan_shared_resources,
    set_shared_resources,
)
from datashare_python.exceptions import DependencyInjectionError
from datashare_python.objects import Shared


async def test_set_shared_resources_and_get() -> None:
    shared = Shared()
    shared.set_resource("k", "v")
    returned = await set_shared_resources(shared)
    assert returned is shared
    assert lifespan_shared_resources() is shared


async def test_set_shared_resources_overwrites() -> None:
    first = Shared()
    second = Shared()
    first.set_resource("k", "v1")
    second.set_resource("k", "v2")
    await set_shared_resources(first)
    await set_shared_resources(second)
    assert lifespan_shared_resources() is second


def test_shared_resources_raises_before_set() -> None:
    with pytest.raises(DependencyInjectionError):
        lifespan_shared_resources()


def test_shared_resources_field_is_frozen() -> None:
    shared = Shared()
    shared.set_resource("k", "v")
    with pytest.raises(Exception):  # noqa: B017, PT011
        shared._resources = {}


def test_get_resource_existing_key() -> None:
    shared = Shared()
    shared.set_resource("k", "v")
    assert shared.get_resource("k") == "v"


def test_get_resource_missing_key_with_default() -> None:
    shared = Shared()
    assert shared.get_resource("missing", "default") == "default"


async def test_async_set_resource() -> None:
    shared = Shared()
    await shared.async_set_resource("k", "v")
    assert shared.get_resource("k") == "v"


async def test_async_set_resource_overwrites() -> None:
    shared = Shared()
    shared.set_resource("k", "old")
    await shared.async_set_resource("k", "new")
    assert shared.get_resource("k") == "new"


async def test_async_pop_resource_missing_key_with_default() -> None:
    shared = Shared()
    assert await shared.async_pop_resource("missing", None) is None


def test_call_component_teardown_on_key_eviction() -> None:
    shared = Shared(1, eviction_callback=component_teardown)

    first_resource = MagicMock()
    second_resource = MagicMock()

    shared.set_resource("first", first_resource)
    shared.set_resource("second", second_resource)

    assert shared._resources.keys() == ["second"]

    first_resource.__exit__.assert_called_once()


@pytest.mark.parametrize(
    ("provided_args", "kwargs", "maybe_output"),
    [
        ({}, {}, None),
        ({"a": "a"}, {}, None),
        ({"a": "a"}, {"b": "b"}, "a-b-c"),
        ({"a": "a", "b": "b"}, {"c": "not-your-average-c"}, "a-b-not-your-average-c"),
    ],
)
def test_add_missing_args(
    provided_args: dict[str, Any],
    kwargs: dict[str, Any],
    maybe_output: str | None,
) -> None:
    # Given
    def fn(a: str, b: str, c: str = "c") -> str:
        return f"{a}-{b}-{c}"

    # When
    all_args = add_missing_args(fn, args=provided_args, **kwargs)
    # Then
    if maybe_output is not None:
        output = fn(**all_args)
        assert output == maybe_output
    else:
        with pytest.raises(TypeError):
            fn(**all_args)
