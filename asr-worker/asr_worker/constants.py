ASR_WORKER_NAME = "asr-worker"

ONE_MINUTE = 60

TEN_MINUTES = ONE_MINUTE * 10

ASR_TASK_QUEUE = "transcription-tasks"

PARAKEET = "parakeet"

DEFAULT_TEMPORAL_ADDRESS = "temporal:7233"

RESPONSE_SUCCESS = "success"

RESPONSE_ERROR = "error"

TRANSCRIPTION_METADATA_KEY = "transcription"
TRANSCRIPTION_METADATA_VALUE = "transcription.json"

ASR_WORKFLOW = "asr.transcription"
GET_CONFIG_ACTIVITY = "asr.transcription.config"
PREPROCESS_ACTIVITY = "asr.transcription.preprocess"
SEARCH_AUDIOS_ACTIVITY = "asr.transcription.search_audios"
RUN_INFERENCE_ACTIVITY = "asr.transcription.infer"
POSTPROCESS_ACTIVITY = "asr.transcription.postprocess"

SUPPORTED_CONTENT_TYPES = {
    "audio/aac",
    "audio/aiff",
    "audio/mp4",
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
