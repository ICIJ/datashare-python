from caul_core import ASRModel, Preprocessor

from .asr_preprocessor import ASRPreprocessorMixin


@Preprocessor.register(ASRModel.FASTER_WHISPER)
class FasterWhisperPreprocessor(ASRPreprocessorMixin): ...
