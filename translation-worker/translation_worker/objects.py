from enum import StrEnum

from aiostream import Stream
from argostranslate.sbd import (
    MiniSBDSentencizer,
    SpacySentencizerSmall,
    StanzaSentencizer,
)
from argostranslate.tokenizer import BPETokenizer, SentencePieceTokenizer
from ctranslate2 import Translator
from datashare_python.constants import CPU
from datashare_python.objects import ArbitraryTypesConfig, BasePayload, WorkerResponse
from pydantic import BaseModel, Field

from .constants import (
    TRANSLATION_CPU_TASK_QUEUE,
    TRANSLATION_GPU_TASK_QUEUE,
    TRANSLATION_TASK_NAME,
)


class TaskQueues(StrEnum):
    CPU = TRANSLATION_CPU_TASK_QUEUE
    GPU = TRANSLATION_GPU_TASK_QUEUE


class TranslationConfig(BaseModel):
    """Translation task config. Refer to ctranslate2 docs for explanation
    of related params
    """

    task: str = Field(default=TRANSLATION_TASK_NAME, frozen=True)
    device: str = Field(default=CPU, frozen=True)
    batch_size: int = 16
    max_parallel_batches: int = 8
    # ctranslate2 params
    beam_size: int = 4
    num_hypotheses: int = 1
    inter_threads: int = 1
    intra_threads: int = 0
    compute_type: str = "auto"


class TranslationRequest(BasePayload):
    project: str
    target_language: str
    translation_config: TranslationConfig = Field(default_factory=TranslationConfig)


class TranslationResponse(WorkerResponse):
    num_translations: int = 0


class AsyncEsDocumentIteratorByLanguage(BaseModel):
    model_config = ArbitraryTypesConfig
    language: str
    document_iter: Stream


class BatchSentence(BaseModel):
    doc_id: str
    root_document: str
    sentence_index: int
    sentence: str


class SentencesBatch(BaseModel):
    language: str
    sentences: list[BatchSentence] = Field(default_factory=list)


class TranslationEnsemble(BaseModel):
    model_config = ArbitraryTypesConfig
    tokenizer: SentencePieceTokenizer | BPETokenizer
    sentencizer: StanzaSentencizer | MiniSBDSentencizer | SpacySentencizerSmall
    translator: Translator
    target_prefix: str = ""
