from enum import StrEnum

from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, DOC_ROOT_ID


class TaskQueue(StrEnum):
    IO = "translation.io"
    INFERENCE = "translation.inference"


class TorchDevice(StrEnum):
    CPU = "cpu"
    GPU = "cuda"


TRANSLATION_TASK_NAME = "translation"

TRANSLATION_WORKER_NAME = "translation-worker"
TRANSLATION_WORKFLOW_NAME = "translation"

CONTENT_LENGTH = "content_length"

TRANSLATION_DOC_SOURCES = [DOC_CONTENT, DOC_ROOT_ID, DOC_LANGUAGE]
BATCHING_DOC_SOURCES = TRANSLATION_DOC_SOURCES[1:] + [CONTENT_LENGTH]
