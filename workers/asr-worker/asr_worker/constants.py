ASR_WORKER_NAME = "asr-worker"

ASR_TASK_QUEUE = "transcription-tasks"

PARAKEET = "parakeet"

DEFAULT_TEMPORAL_ADDRESS = "temporal:7233"

RESPONSE_SUCCESS = "success"

RESPONSE_ERROR = "error"

ASR_WORKFLOW = "asr.transcription"
GET_CONFIG_ACTIVITY = "asr.transcription.config"
PREPROCESS_ACTIVITY = "asr.transcription.preprocess"
SEARCH_AUDIOS_ACTIVITY = "asr.transcription.search-audios"
RUN_INFERENCE_ACTIVITY = "asr.transcription.infer"
POSTPROCESS_ACTIVITY = "asr.transcription.postprocess"
INDEX_TRANSCRIPTION_ACTIVITY = "asr.transcription.index"

SUPPORTED_CONTENT_TYPES = {
    "audio/aac",
    "audio/aiff",
    "audio/mp4",
    "audio/mpeg",
    "audio/ogg",
    "audio/vnd.wave",
    "audio/wav",
    "audio/wave",
    "audio/x-wav",
    "audio/x-pn-wav",
    "video/mp4",
    "video/mpeg",
    "video/mov",
}
