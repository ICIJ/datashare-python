try:
    from .parakeet import ParakeetInferenceRunner
except ModuleNotFoundError:
    ParakeetInferenceRunner = None
try:
    from .faster_whisper import FasterWhisperInferenceRunner
except ModuleNotFoundError:
    FasterWhisperInferenceRunner = None
try:
    from .fireredasr import FireRedASR2InferenceRunner
except ModuleNotFoundError:
    FireRedASR2InferenceRunner = None
try:
    from .parakeet_trt import ParakeetTrtInferenceRunner
except ModuleNotFoundError:
    ParakeetTrtInferenceRunner = None
try:
    from .whisper_trt import WhisperTrtInferenceRunner
except ModuleNotFoundError:
    WhisperTrtInferenceRunner = None
