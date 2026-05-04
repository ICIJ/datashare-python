from typing import Annotated, Any

from datashare_python.config import WorkerConfig
from datashare_python.objects import DatashareModel
from pydantic import BaseModel, BeforeValidator, Field
from pydantic_extra_types.language_code import LanguageName

from translation_worker.constants import TorchDevice


class TranslationWorkerConfig(WorkerConfig):
    device: TorchDevice = Field(default=TorchDevice.CPU, frozen=True)

    batch_size: int = 16
    max_parallel_batches: int = 8
    max_batch_byte_len: int = 1000000
    # ctranslate2 params
    beam_size: int = 4
    inter_threads: int = 1
    intra_threads: int = 0
    compute_type: str = "auto"  # quantization


WORKER_CONFIG_CLS = TranslationWorkerConfig


def _to_language_name(value: Any) -> Any:
    if isinstance(value, str):
        return value.title()
    return value


class TranslationArgs(DatashareModel):
    project: str
    target_language: Annotated[LanguageName, BeforeValidator(_to_language_name)]


class TranslationResponse(DatashareModel):
    n_translations: int = 0


class BatchSentence(BaseModel):
    doc_id: str
    root_document: str
    sentence_index: int
    sentence: str
