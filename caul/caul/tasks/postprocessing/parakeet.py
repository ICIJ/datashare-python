from caul_core import ASRModel, Postprocessor

from .asr_postprocessor import PostprocessorMixin


@Postprocessor.register(ASRModel.PARAKEET)
class ParakeetPostprocessor(PostprocessorMixin): ...
