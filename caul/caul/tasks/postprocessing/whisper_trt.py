from caul_core import ASRModel, Postprocessor

from .asr_postprocessor import PostprocessorMixin


@Postprocessor.register(ASRModel.WHISPER_TRT)
class WhisperTrtPostprocessor(PostprocessorMixin): ...
