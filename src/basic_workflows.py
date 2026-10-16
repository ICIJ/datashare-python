# --8<-- [start:workflows]
from datetime import timedelta

from temporalio import workflow

from .activities import hello_user


@workflow.defn(name="hello-user")  # (1)!
class HelloUserWorkflow:
    @workflow.run  # (2)!
    async def run_hello_world_workflow(self, args: dict) -> str:
        user = args.get("user")  # (3)!
        return await workflow.execute_activity(
            hello_user,  # (4)!
            user,
            start_to_close_timeout=timedelta(seconds=10),
        )


WORKFLOWS = [HelloUserWorkflow]  # (5)!
# --8<-- [end:workflows]
