from .activities import hello_user


def run_hello_world_workflow(args: dict) -> str:
    user = args.get("user")
    return hello_user(user)
