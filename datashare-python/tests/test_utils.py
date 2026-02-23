from datashare_python.utils import positional_args_only
from temporalio import activity


@positional_args_only
def hello_world_keyword(*, who: str) -> str:
    return f"hello {who}"


def test_keyword_safe_activity() -> None:
    # When
    try:
        activity.defn(hello_world_keyword)
    except Exception as e:
        raise AssertionError(
            "couldn't create activity from keyword only function "
        ) from e
