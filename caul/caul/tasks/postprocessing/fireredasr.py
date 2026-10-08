from caul_core import ASRModel, Postprocessor

from .asr_postprocessor import PostprocessorMixin


@Postprocessor.register(ASRModel.FIREREDASR2_AED)
class FireRedASR2Postprocessor(PostprocessorMixin): ...
