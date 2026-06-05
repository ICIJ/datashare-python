from enum import StrEnum

MARKDOWN_METADATA_KEY = "extract.markdown"
MARKDOWN_DIRNAME = "markdown"


class TaskQueue(StrEnum):
    WORKFLOWS = "datashare.workflows"
    IO = "extract.io"
    MARKDOWN_GPU = "extract.markdown.gpu"
    MARKDOWN_CPU = "extract.markdown.cpu"


class TorchDevice(StrEnum):
    CPU = "cpu"
    GPU = "cuda"

    @property
    def md_extract_queue(self) -> TaskQueue:
        match self:
            case TorchDevice.GPU:
                return TaskQueue.MARKDOWN_GPU
            case TorchDevice.CPU:
                return TaskQueue.MARKDOWN_CPU
            case _:
                raise ValueError(f"unsupported TorchDevice {self}")
