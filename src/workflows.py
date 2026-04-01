from datetime import timedelta

from temporalio import workflow

from .activities import hello


@workflow.defn(name="hello-world")
class HelloWorld:
    @workflow.run
    async def run(self, person: str) -> str:
        return await workflow.execute_activity(
            hello,
            person,
            start_to_close_timeout=timedelta(seconds=10),
        )
