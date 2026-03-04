from .activities import hello_user


def run_workflow(user: dict | None) -> str:
    return hello_user(user)
