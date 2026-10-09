from enum import StrEnum


class VadModel(StrEnum):
    SILERO_MODEL = "silero_vad"
    PYANNOTE_MODEL = "pyannote/segmentation-3.0"


class TorchDevice(StrEnum):
    CPU = "cpu"
    GPU = "cuda"
    MPS = "mps"


DEFAULT_SAMPLE_RATE = 16000
DEFAULT_BATCH_SIZE = 32
DEFAULT_MAX_FRAMES = 25 * 60 * DEFAULT_SAMPLE_RATE  # twenty-five minutes
DEFAULT_LARGE_FILE_THRESHOLD_BYTES = 30 * 1024 * 1024  # 30mb

# Languages
PARAKEET_TDT_0_6B_V3_LANGUAGES = {
    "bg",
    "hr",
    "cs",
    "da",
    "nl",
    "en",
    "et",
    "fi",
    "fr",
    "de",
    "el",
    "hu",
    "it",
    "lv",
    "lt",
    "mt",
    "pl",
    "pt",
    "ro",
    "sk",
    "sl",
    "es",
    "sv",
    "ru",
    "uk",
}
FIREREDASR2_LANGUAGES = {"zh"}
WHISPER_TRT_LANGUAGES = {
    "en": "english",
    "zh": "chinese",
    "de": "german",
    "es": "spanish",
    "ru": "russian",
    "ko": "korean",
    "fr": "french",
    "ja": "japanese",
    "pt": "portuguese",
    "tr": "turkish",
    "pl": "polish",
    "ca": "catalan",
    "nl": "dutch",
    "ar": "arabic",
    "sv": "swedish",
    "it": "italian",
    "id": "indonesian",
    "hi": "hindi",
    "fi": "finnish",
    "vi": "vietnamese",
    "he": "hebrew",
    "uk": "ukrainian",
    "el": "greek",
    "ms": "malay",
    "cs": "czech",
    "ro": "romanian",
    "da": "danish",
    "hu": "hungarian",
    "ta": "tamil",
    "no": "norwegian",
    "th": "thai",
    "ur": "urdu",
    "hr": "croatian",
    "bg": "bulgarian",
    "lt": "lithuanian",
    "la": "latin",
    "mi": "maori",
    "ml": "malayalam",
    "cy": "welsh",
    "sk": "slovak",
    "te": "telugu",
    "fa": "persian",
    "lv": "latvian",
    "bn": "bengali",
    "sr": "serbian",
    "az": "azerbaijani",
    "sl": "slovenian",
    "kn": "kannada",
    "et": "estonian",
    "mk": "macedonian",
    "br": "breton",
    "eu": "basque",
    "is": "icelandic",
    "hy": "armenian",
    "ne": "nepali",
    "mn": "mongolian",
    "bs": "bosnian",
    "kk": "kazakh",
    "sq": "albanian",
    "sw": "swahili",
    "gl": "galician",
    "mr": "marathi",
    "pa": "punjabi",
    "si": "sinhala",
    "km": "khmer",
    "sn": "shona",
    "yo": "yoruba",
    "so": "somali",
    "af": "afrikaans",
    "oc": "occitan",
    "ka": "georgian",
    "be": "belarusian",
    "tg": "tajik",
    "sd": "sindhi",
    "gu": "gujarati",
    "am": "amharic",
    "yi": "yiddish",
    "lo": "lao",
    "uz": "uzbek",
    "fo": "faroese",
    "ht": "haitian creole",
    "ps": "pashto",
    "tk": "turkmen",
    "nn": "nynorsk",
    "mt": "maltese",
    "sa": "sanskrit",
    "lb": "luxembourgish",
    "my": "myanmar",
    "bo": "tibetan",
    "tl": "tagalog",
    "mg": "malagasy",
    "as": "assamese",
    "tt": "tatar",
    "haw": "hawaiian",
    "ln": "lingala",
    "ha": "hausa",
    "ba": "bashkir",
    "jw": "javanese",
    "su": "sundanese",
    "yue": "cantonese",
}

# Silero
SILERO_TORCH_HUB_REPO = "snakers4/silero-vad"

# Parakeet

PARAKEET_MODEL_REF = "nvidia/parakeet-tdt-0.6b-v3"
PARAKEET_INFERENCE_MAX_DURATION_S = (
    20 * 60  # actually 24, but we want to give ourselves some room to avoid CUDA OOMs
)
PARAKEET_INFERENCE_MAX_FRAMES = PARAKEET_INFERENCE_MAX_DURATION_S * DEFAULT_SAMPLE_RATE

# Faster Whisper (SYSTRAN/faster-whisper)

FASTER_WHISPER_COMPUTE_TYPE_DEFAULT = "float32"
FASTER_WHISPER_WORD_TIMESTAMPS_DEFAULT = True
FASTER_WHISPER_BEAM_SIZE_DEFAULT = 5
FASTER_WHISPER_BEST_OF_DEFAULT = 5
FASTER_WHISPER_PATIENCE_DEFAULT = 1
FASTER_WHISPER_LENGTH_PENALTY_DEFAULT = 1
FASTER_WHISPER_REPETITION_PENALTY_DEFAULT = 1
FASTER_WHISPER_NO_REPEAT_NGRAM_SIZE_DEFAULT = 0
FASTER_WHISPER_LOG_PROB_THRESHOLD_DEFAULT = -1.0
FASTER_WHISPER_NO_SPEECH_THRESHOLD_DEFAULT = 0.6
FASTER_WHISPER_COMPRESSION_RATIO_THRESHOLD_DEFAULT = 2.4
FASTER_WHISPER_CONDITION_ON_PREVIOUS_TEXT_DEFAULT = False
FASTER_WHISPER_PROMPT_RESET_ON_TEMPERATURE_DEFAULT = 0.5
FASTER_WHISPER_TEMPERATURES_DEFAULT = [0.0]
FASTER_WHISPER_SUPPRESS_BLANK_DEFAULT = True
FASTER_WHISPER_WITHOUT_TIMESTAMPS_DEFAULT = True
FASTER_WHISPER_MAX_INITIAL_TIMESTAMP_DEFAULT = 0.0
FASTER_WHISPER_PREPEND_PUNCTUATIONS_DEFAULT = "\"'‘’“¿([{—-"
FASTER_WHISPER_APPEND_PUNCTUATIONS_DEFAULT = "\"'’”.。,，!！?？:：””)]、結}"
FASTER_WHISPER_MULTILINGUAL_DEFAULT = False
FASTER_WHISPER_CLIP_TIMESTAMPS_DEFAULT = []
FASTER_WHISPER_LARGE_V3_TURBO_SUPPRESSED_TOKENS = [
    1,
    2,
    7,
    8,
    9,
    10,
    14,
    25,
    26,
    27,
    28,
    29,
    31,
    58,
    59,
    60,
    61,
    62,
    63,
    90,
    91,
    92,
    93,
    359,
    503,
    522,
    542,
    873,
    893,
    902,
    918,
    922,
    931,
    1350,
    1853,
    1982,
    2460,
    2627,
    3246,
    3253,
    3268,
    3536,
    3846,
    3961,
    4183,
    4667,
    6585,
    6647,
    7273,
    9061,
    9383,
    10428,
    10929,
    11938,
    12033,
    12331,
    12562,
    13793,
    14157,
    14635,
    15265,
    15618,
    16553,
    16604,
    18362,
    18956,
    20075,
    21675,
    22520,
    26130,
    26161,
    26435,
    28279,
    29464,
    31650,
    32302,
    32470,
    36865,
    42863,
    47425,
    49870,
    50254,
    50258,
    50359,
    50360,
    50361,
    50362,
    50363,
]

# Parakeet TRT
PARAKEET_TRT_MODEL_REF = "ICIJ/parakeet-v3-trt"

# FireRedASR2s

FIREREDASR2_MODEL_HUB_PREFIX = "FireRedTeam/FireRed"
FIREREDASR2_INFERENCE_MAX_DURATION_S = 60
FIREREDASR2_INFERENCE_MAX_FRAMES = (
    FIREREDASR2_INFERENCE_MAX_DURATION_S * DEFAULT_SAMPLE_RATE
)
FIREREDASR2_USE_HALF_DEFAULT = False
FIREREDASR2_BEAM_SIZE_DEFAULT = 3
FIREREDASR2_NBEST_DEFAULT = 1
FIREREDASR2_DECODE_MAX_LEN_DEFAULT = 0
FIREREDASR2_SOFTMAX_SMOOTHING_DEFAULT = 1.25
FIREREDASR2_AED_LENGTH_PENALTY_DEFAULT = 0.6
FIREREDASR2_EOS_PENALTY_DEFAULT = 1.0
FIREREDASR2_RETURN_TIMESTAMP_DEFAULT = True

# Whisper TRT

WHISPER_TRT_N_FFT = 400
WHISPER_TRT_HOP_LENGTH = 160
WHISPER_TRT_WORLD_SIZE = 1  # TODO: Explain what this is
WHISPER_TRT_EOT_TOKEN = "<|endoftext|>"
WHISPER_TRT_ML_TOKENIZER_VOCAB_SIZE_THRESHOLD = 51865
WHISPER_TRT_PROMPT_PREFIX = "<|startoftranscript|><|en|><|transcribe|>"
WHISPER_TRT_MAX_NEW_TOKENS = 3000
WHISPER_TRT_MAX_MEL_PADDING_LEN = 3000
WHISPER_TRT_PATTERN_STR = (
    r"""'s|'t|'re|'ve|'m|'ll|'d| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+"""
)
WHISPER_TRT_PAD_TOKEN_ID = 50256
WHISPER_TRT_MAX_FRAMES = 480000  # 30 secs

WHISPER_TRT_N_MELS = 128
WHISPER_TRT_DTYPE = "float16"

WHISPER_TRT_PREPROCESSOR_CLAMP_MIN = 1e-10
WHISPER_TRT_PREPROCESSOR_LOG_RANGE_MAX_SHIFT = 8.0
WHISPER_TRT_PREPROCESSOR_LOG_RANGE_NORMALIZER = 4.0

WHISPER_TRT_ENCODER_DOWNSAMPLING_FACTOR = 2

WHISPER_TRT_DECODER_BATCH_SIZE = 8
WHISPER_TRT_DECODER_BEAM_WIDTH = 1
WHISPER_TRT_DECODER_NUM_HIDDEN_LAYERS = 2
WHISPER_TRT_DECODER_HIDDEN_SIZE = 1280
WHISPER_TRT_DECODER_VOCAB_SIZE = 51866
WHISPER_TRT_DECODER_NUM_HEADS = 20
WHISPER_TRT_RETURN_TIMESTAMPS = True
WHISPER_TRT_DECODER_GPT_ATTENTION_PLUGIN = "auto"
WHISPER_TRT_DECODER_PAGED_KV_CACHE = True
WHISPER_TRT_DECODER_HAS_POSITION_EMBEDDING = True
WHISPER_TRT_DECODER_CROSS_ATTENTION = True
WHISPER_TRT_DECODER_HAS_TOKEN_TYPE_EMBEDDING = False
WHISPER_TRT_DECODER_DEBUG_MODE = False
WHISPER_TRT_DECODER_REMOVE_INPUT_PADDING = False
WHISPER_TRT_SPECIAL_TOKENS = [
    "<|endoftext|>",
    "<|startoftranscript|>",
    *[f"<|{lang}|>" for lang in list(WHISPER_TRT_LANGUAGES.keys())],
    "<|translate|>",
    "<|transcribe|>",
    "<|startoflm|>",
    "<|startofprev|>",
    "<|nospeech|>",
    "<|notimestamps|>",
    *[f"<|{i * 0.02:.2f}|>" for i in range(1501)],
]
WHISPER_TRT_ENCODER_INPUT_FEATURES = "input_features"
WHISPER_TRT_ENCODER_INPUT_LENGTHS = "input_lengths"
WHISPER_TRT_ENCODER_POSITION_IDS = "position_ids"


# Silero

SILERO_VAD_MODEL = "silero_vad"
