import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

from caul_core import (
    PARAKEET_MODEL_REF,
    ASRModel,
    ASRResult,
    Error,
    InferenceRunner,
    ParakeetInferenceRunnerConfig,
    ProcessedAudioSegment,
    TorchDevice,
)
from icij_common.registrable import FromConfig

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import torch
    from nemo.collections.asr.parts.mixins import TranscribeConfig


@InferenceRunner.register(ASRModel.PARAKEET)
class ParakeetInferenceRunner(InferenceRunner):
    """Inference handler for NVIDIA's Parakeet family of ASR models.

    Supports up to 24 minutes of audio (batched or unbatched) in a single pass. Assumes
    that audio inputs (wav files or tensors) are single-channel with a sample rate of
    16000—this last is very important for segmenting.
    """

    def __init__(
        self,
        model_name: str = PARAKEET_MODEL_REF,
        device: TorchDevice = TorchDevice.CPU,
        batch_size: int = 4,
    ):
        super().__init__(device)
        self.model_name = model_name
        self._model = None
        self.__transcribe_config = None
        self._batch_size = batch_size

    @classmethod
    def _from_config(
        cls, config: ParakeetInferenceRunnerConfig, **extras
    ) -> FromConfig:
        return cls(
            model_name=config.model_name,
            **extras,
        )

    @property
    def _transcribe_config(self) -> "TranscribeConfig":
        if self.__transcribe_config is None:
            from nemo.collections.asr.parts.mixins import TranscribeConfig
            from nemo.collections.asr.parts.mixins.transcription import (
                InternalTranscribeConfig,
            )

            self.__transcribe_config = TranscribeConfig(
                use_lhotse=False,
                batch_size=self._batch_size,
                timestamps=True,
                return_hypotheses=True,
                # Bug in Nemo's AudioToBPEDataset—by default TranscribeConfig spawns 2
                # DataLoader workers, but AudioToBPEDataset defines a class
                # TokenizerWrapper inside __init__, meaning it can't be pickled.
                num_workers=0,
                _internal=InternalTranscribeConfig(device=self._torch_device),
            )

        return self.__transcribe_config

    def __enter__(self):
        import nemo.collections.asr as nemo_asr

        self._model = nemo_asr.models.ASRModel.from_pretrained(
            self.model_name, map_location=self._torch_device
        ).eval()
        return self

    def _transcribe(
        self, audio_inputs: Iterable["torch.Tensor"], **kwargs
    ) -> ASRResult:
        """Transcribe normalized audio tensors

        :param audio_inputs: input tensors
        :return: transcription results
        """
        return self._model.transcribe(
            audio_inputs,
            timestamps=True,
            override_config=self._transcribe_config,
        )

    def process(
        self, inputs: Iterable[tuple[ProcessedAudioSegment, ...]], *args, **kwargs
    ) -> Iterable[ASRResult | Error]:
        """Transcribe a batch of audio tensors or file names of max duration <= 20 min

        :param inputs: List of np.ndarray or torch.Tensor or str, or singleton of same
            types
        :return: List of results
        """
        for input_batch in inputs:
            if not input_batch:
                continue
            if hasattr(input_batch[0], "tensor"):
                audios = [i.tensor.to(self._torch_device) for i in input_batch]
            else:
                audios = [str(i.path) for i in input_batch]

            hypotheses = self._transcribe(audios)
            for idx, hyps in enumerate(hypotheses):
                best_hyp = hyps

                if isinstance(best_hyp, (list, tuple)):
                    best_hyp = hyps[0]

                input_ordering_idx = input_batch[idx].metadata.index
                model_result = ASRResult.from_parakeet_hypothesis(
                    best_hyp, index=input_ordering_idx
                )
                yield model_result
