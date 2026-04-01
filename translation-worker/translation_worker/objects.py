from dataclasses import dataclass
from enum import StrEnum

from argostranslate.sbd import (
    MiniSBDSentencizer,
    SpacySentencizerSmall,
    StanzaSentencizer,
)
from argostranslate.tokenizer import BPETokenizer, SentencePieceTokenizer
from ctranslate2 import Translator
from datashare_python.objects import BasePayload
from pydantic import BaseModel, Field

from .constants import (
    CPU,
    TRANSLATION_CPU_TASK_QUEUE,
    TRANSLATION_GPU_TASK_QUEUE,
    TRANSLATION_TASK_NAME,
)


class TaskQueues(StrEnum):
    CPU = TRANSLATION_CPU_TASK_QUEUE
    GPU = TRANSLATION_GPU_TASK_QUEUE


class TranslationWorkerConfig(BaseModel):
    task: str = Field(default=TRANSLATION_TASK_NAME, frozen=True)
    device: str = Field(default=CPU, frozen=True)
    batch_size: int = 16
    max_parallel_batches: int = 8
    max_batch_byte_len: int = 1000000
    # ctranslate2 params
    beam_size: int = 4
    inter_threads: int = 1
    intra_threads: int = 0
    compute_type: str = "auto"  # quantization


class TranslationRequest(BasePayload):
    project: str
    target_language: str


class TranslationResponse(BasePayload):
    num_translations: int = 0


class BatchSentence(BaseModel):
    doc_id: str
    root_document: str
    sentence_index: int
    sentence: str


@dataclass(frozen=True)
class TranslationEnsemble:
    tokenizer: SentencePieceTokenizer | BPETokenizer
    sentencizer: StanzaSentencizer | MiniSBDSentencizer | SpacySentencizerSmall
    translator: Translator
    target_prefix: str = ""
