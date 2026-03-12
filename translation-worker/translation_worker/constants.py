from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, DOC_ROOT_ID

TRANSLATION_TASK_NAME = "translation"

TRANSLATION_WORKER_NAME = "translation-worker"

TRANSLATION_CPU_TASK_QUEUE = "translation-cpu-tasks"

TRANSLATION_GPU_TASK_QUEUE = "translation-gpu-tasks"

TRANSLATION_WORKFLOW_NAME = "translation"

LANGUAGE_ALPHA_CODE_ACTIVITY_NAME = "language-alpha-code-activity"

TRANSLATION_BATCHING_ACTIVITY_NAME = "translation-batches-activity"

TRANSLATION_TRANSLATE_ACTIVITY_NAME = "translation-translate-activity"

TRANSLATION_DOC_SOURCES = [DOC_CONTENT, DOC_ROOT_ID, DOC_LANGUAGE]
