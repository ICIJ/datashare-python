from enum import StrEnum
from typing import Self

from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, DOC_ROOT_ID

from .config import TranslationConfig
from .objects import TranslationModel


class TaskQueue(StrEnum):
    WORKFLOWS = "datashare.workflows"
    IO = "translation.io"
    TORCH_INFERENCE = "translation.inference.torch"
    C2TRANSLATE_INFERENCE = "translation.inference.c2translate"

    @classmethod
    def inference_queue(cls, config: TranslationConfig) -> Self:
        model = config.translator.model
        match model:
            case TranslationModel.ARGOS:
                return TaskQueue.C2TRANSLATE_INFERENCE
            case TranslationModel.HUNYUAN:
                return TaskQueue.TORCH_INFERENCE
            case _:
                raise ValueError(f"unknown translation model {model}")


TRANSLATION_TASK_NAME = "translation"

TRANSLATION_WORKER_NAME = "translation-worker"
TRANSLATION_WORKFLOW_NAME = "translation"

DOC_CONTENT_TEXT_LENGTH = "contentTextLength"
TRANSLATION_DOC_SOURCES = [DOC_CONTENT, DOC_ROOT_ID, DOC_LANGUAGE]
BATCHING_DOC_SOURCES = TRANSLATION_DOC_SOURCES[1:] + [DOC_CONTENT_TEXT_LENGTH]
