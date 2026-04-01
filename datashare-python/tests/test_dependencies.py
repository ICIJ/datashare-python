from typing import Any

import pytest
from datashare_python.dependencies import add_missing_args


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
