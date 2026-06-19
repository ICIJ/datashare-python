from enum import StrEnum

from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, DOC_ROOT_ID


class TaskQueue(StrEnum):
    WORKFLOWS = "datashare.workflows"
    IO = "translation.io"
    INFERENCE = "translation.inference"


class TorchDevice(StrEnum):
    CPU = "cpu"
    GPU = "cuda"


TRANSLATION_TASK_NAME = "translation"

TRANSLATION_WORKER_NAME = "translation-worker"
TRANSLATION_WORKFLOW_NAME = "translation"

DOC_CONTENT_TEXT_LENGTH = "contentTextLength"
TRANSLATION_DOC_SOURCES = [DOC_CONTENT, DOC_ROOT_ID, DOC_LANGUAGE]
BATCHING_DOC_SOURCES = TRANSLATION_DOC_SOURCES[1:] + [DOC_CONTENT_TEXT_LENGTH]

DEFAULT_HUNYUAN_MODEL_REF = "tencent/Hunyuan-MT-Chimera-7B"
HUNYUAN_SHARED_KEY = "hunyuan_translator"

HUNYUAN_LANGUAGES = [
    # "en",
    "zh",
    # "fr",
    # "pt",
    # "es",
    # "ja",
    # "tr",
    # "ru",
    # "ar",
    # "ko",
    # "th",
    # "it",
    # "de",
    # "vi",
    # "ms",
    # "id",
    # "tl",
    # "hi",
    # "pl",
    # "cs",
    # "nl",
    # "km",
    # "my",
    # "fa",
    # "gu",
    # "ur",
    # "te",
    # "mr",
    # "he",
    # "bn",
    # "ta",
    # "uk",
    # "bo",
    # "kk",
    # "mn",
    # "ug",
]
