# --8<-- [start:workflows]
from datetime import timedelta

from temporalio import workflow

from .activities import hello_user


@workflow.defn(name="hello-user")  # (1)!
class HelloUserWorkflow:
    @workflow.run  # (2)!
    async def run(self, user: dict | None) -> str:
        return await workflow.execute_activity(
            hello_user,  # (3)!
            user,
            start_to_close_timeout=timedelta(seconds=10),
        )


WORKFLOWS = [HelloUserWorkflow]  # (4)!
# --8<-- [end:workflows]
