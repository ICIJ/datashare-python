import logging
from collections.abc import Iterable
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from caul_core import (
    FIREREDASR2_MODEL_HUB_PREFIX,
    ASRModel,
    ASRResult,
    Error,
    FireRedASR2InferenceRunnerConfig,
    FireRedASR2ModelRef,
    FireRedASR2ModelTag,
    InferenceRunner,
    ProcessedAudioSegment,
    TorchDevice,
)
from icij_common.registrable import FromConfig

from caul.utils import to_filesystem

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import torch
    from fireredasr2s.fireredasr2 import FireRedAsr2, FireRedAsr2Config


def fireredasr2_from_pretrained(
    model_ref: FireRedASR2ModelRef,
    *,
    cache_dir: Path | None = None,
    model_tag: FireRedASR2ModelTag = FireRedASR2ModelTag.AED,
    config: "FireRedAsr2Config | None" = None,
) -> "FireRedAsr2":
    """Wrap FireRedAsr2.from_pretrained, downloading the model from HuggingFace Hub
    if not already available in cache_dir.

    :param model_ref: One of ASR2, VAD, LID, or Punc
    :param cache_dir: Directory to download model to
    :param model_tag: For ASR models, can be either AED or LLM
    :param config: Configuration options for model
    """
    from fireredasr2s.fireredasr2 import (
        FireRedAsr2,
        FireRedAsr2Config,
    )
    from huggingface_hub import (
        snapshot_download,
    )
    from huggingface_hub.constants import (
        HF_HUB_CACHE,
    )

    if cache_dir is None:
        cache_dir = HF_HUB_CACHE

    if config is None:
        config = FireRedAsr2Config()

    if model_tag is not None:
        model_ref = f"{model_ref}-{model_tag}"

    repo_id = FIREREDASR2_MODEL_HUB_PREFIX + model_ref.upper()

    model_dir = snapshot_download(repo_id=repo_id, cache_dir=cache_dir)
    return FireRedAsr2.from_pretrained(model_tag, model_dir, config)


def inference_config_to_fire_red_asr_model(
    inference_config: FireRedASR2InferenceRunnerConfig,
    *,
    use_gpu: bool = True,
    cache_dir: Path | None = None,
) -> "torch.Module":
    from fireredasr2s.fireredasr2 import (
        FireRedAsr2Config,
    )

    asr_config = FireRedAsr2Config(
        use_gpu=use_gpu,
        use_half=inference_config.use_half,
        beam_size=inference_config.beam_size,
        nbest=inference_config.nbest,
        decode_max_len=inference_config.decode_max_len,
        softmax_smoothing=inference_config.softmax_smoothing,
        aed_length_penalty=inference_config.aed_length_penalty,
        eos_penalty=inference_config.eos_penalty,
        return_timestamp=inference_config.return_timestamp,
    )

    return fireredasr2_from_pretrained(
        model_ref=FireRedASR2ModelRef.ASR2,
        model_tag=FireRedASR2ModelTag.AED,
        cache_dir=cache_dir,
        config=asr_config,
    )


@InferenceRunner.register(ASRModel.FIREREDASR2_AED)
class FireRedASR2InferenceRunner(InferenceRunner):
    """Inference runner for the FireRedASR2 AED model.

    Transcribes Chinese-language (and multilingual) audio. Expects 16 kHz
    mono wav files or tensors (segments up to 60 s). Requires fireredasr2s to
    be installed from https://github.com/FireRedTeam/FireRedASR2S.
    """

    def __init__(
        self,
        config: FireRedASR2InferenceRunnerConfig = None,
        device: TorchDevice = TorchDevice.CPU,
    ):
        super().__init__(device)
        if config is None:
            config = FireRedASR2InferenceRunnerConfig()

        self._config = config
        self._model = None

    @classmethod
    def _from_config(
        cls,
        config: FireRedASR2InferenceRunnerConfig,
        device: TorchDevice = TorchDevice.CPU,
        **extras,
    ) -> FromConfig:
        return cls(config=config, device=device, **extras)

    def __enter__(self):
        use_gpu = self._device is TorchDevice.GPU
        self._model = inference_config_to_fire_red_asr_model(
            self._config, use_gpu=use_gpu
        )
        return self

    def process(
        self,
        inputs: Iterable[tuple[ProcessedAudioSegment, ...]],
        *,
        output_dir: str | Path = None,
        **kwargs,
    ) -> Iterable[ASRResult | Error]:
        """Transcribe batches of preprocessed audio segments.

        :param inputs: batches of PreprocessorOutput (file-backed or tensor)
        :param output_dir: directory to save output files (will default to tempdir if
        not provided
        :return: ASRResult per segment, in batch order
        """
        if isinstance(output_dir, str):
            output_dir = Path(output_dir)
        tmp_dir_ctx = nullcontext()
        if output_dir is None and self._config.tmp_dir_fallback:
            tmp_dir_ctx = TemporaryDirectory()
            output_dir = Path(tmp_dir_ctx.name)
        with tmp_dir_ctx:
            for input_batch in inputs:
                if len(input_batch) == 0:
                    continue
                batch_inputs, batch_paths = zip(
                    *to_filesystem(input_batch, output_dir), strict=False
                )
                batch_paths = [str(p) for p in batch_paths]
                batch_inputs = list(batch_inputs)
                inp_ids = [i.metadata.uuid for i in batch_inputs]
                results = self._model.transcribe(inp_ids, batch_paths)
                for inp, result in zip(batch_inputs, results, strict=True):
                    # FireRedASR2 uses uttid to refer to a unique identifier for each
                    # item in a batch; technically results should be returned in the
                    # order given by uttids, but we use a map to the original index
                    # just to be safe.
                    yield ASRResult.from_fireredasr2_result(
                        result, index=inp.metadata.index
                    )
