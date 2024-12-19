from pathlib import Path

from icij_worker.app import TaskGroup

DATA_DIR = Path(__file__).parent.joinpath(".data")
PYTHON_TASK_GROUP = TaskGroup(name="PYTHON")
