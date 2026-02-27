from pathlib import Path

from .objects import TaskGroup

DATA_DIR = Path(__file__).parent.joinpath(".data")

PYTHON_TASK_GROUP = TaskGroup(name="PYTHON")

DEFAULT_TEMPORAL_ADDRESS = "temporal:7233"

DEFAULT_DS_ADDRESS = "http://localhost:8080"

DEFAULT_NAMESPACE = "datashare-default"
