from caul_core import ASRModel, Postprocessor

from .asr_postprocessor import PostprocessorMixin


@Postprocessor.register(ASRModel.FASTER_WHISPER)
class FasterWhisperPostprocessor(PostprocessorMixin): ...
