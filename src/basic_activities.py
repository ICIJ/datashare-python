# --8<-- [start:activities]
from datashare_python.utils import activity_defn


@activity_defn(name="hello_user")  # (1)!
# --8<-- [start:hello_user_fn]
def hello_user(user: dict | None) -> str:
    greeting = "Hello "
    user = "unknown" if user is None else user["id"]
    return greeting + user


# --8<-- [end:hello_user_fn]
ACTIVITIES = [hello_user]  # (2)!
# --8<-- [end:activities]
