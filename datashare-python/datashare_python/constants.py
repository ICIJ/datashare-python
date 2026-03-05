from pathlib import Path

from .objects import TaskGroup

PACKAGE_DIR = Path(__file__).parent
PACKAGE_ROOT = PACKAGE_DIR.parent

PYTHON_TASK_GROUP = TaskGroup(name="PYTHON")

DEFAULT_TEMPORAL_ADDRESS = "temporal:7233"

DEFAULT_DS_ADDRESS = "http://localhost:8080"

DEFAULT_NAMESPACE = "datashare-default"

CPU = "cpu"

CUDA = "cuda"

MPS = "mps"

MKL = "mkl"
