from enum import StrEnum

from extract_core import PipelineType

MARKDOWN_METADATA_KEY = "extract.markdown"
MARKDOWN_DIRNAME = "markdown"


class TaskQueue(StrEnum):
    WORKFLOWS = "datashare.workflows"
    IO = "extract.io"
    EXTRACT_GPU_MINER_U = "extract.gpu.mineru"
    EXTRACT_CPU_MINER_U = "extract.cpu.mineru"
    EXTRACT_GPU = "extract.gpu"
    EXTRACT_CPU = "extract.cpu"


class TorchDevice(StrEnum):
    CPU = "cpu"
    GPU = "cuda"

    def md_extract_queue(self, pipeline: PipelineType) -> TaskQueue:
        is_mineru = pipeline is PipelineType.MINER_U
        match self:
            case TorchDevice.GPU:
                if is_mineru:
                    return TaskQueue.EXTRACT_GPU_MINER_U
                return TaskQueue.EXTRACT_GPU
            case TorchDevice.CPU:
                if is_mineru:
                    return TaskQueue.EXTRACT_CPU_MINER_U
                return TaskQueue.EXTRACT_CPU
            case _:
                raise ValueError(f"unsupported TorchDevice {self}")
