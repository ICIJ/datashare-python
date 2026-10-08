try:
    from .parakeet import ParakeetPostprocessor
except ModuleNotFoundError:
    ParakeetPostprocessor = None
try:
    from .fireredasr import FireRedASR2Postprocessor
except ModuleNotFoundError:
    FireRedASR2Postprocessor = None
try:
    from .faster_whisper import FasterWhisperPostprocessor
except ModuleNotFoundError:
    FasterWhisperPostprocessor = None
try:
    from .whisper_trt import WhisperTrtPostprocessor
except ModuleNotFoundError:
    WhisperTrtPostprocessor = None
