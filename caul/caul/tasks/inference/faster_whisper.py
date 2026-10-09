import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

from caul_core import (
    DEFAULT_SAMPLE_RATE,
    ASRModel,
    ASRResult,
    Error,
    FasterWhisperInferenceRunnerConfig,
    FSProcessedSegment,
    InferenceRunner,
    MemoryProcessedSegment,
    ProcessedAudioSegment,
    TorchDevice,
)
from icij_common.registrable import FromConfig

from ...utils import load_audio

if TYPE_CHECKING:
    from faster_whisper.transcribe import TranscriptionOptions

logger = logging.getLogger(__name__)


def inference_config_to_transcription_options(
    inference_config: FasterWhisperInferenceRunnerConfig,
) -> "TranscriptionOptions":
    from faster_whisper.transcribe import (
        TranscriptionOptions,
    )

    return TranscriptionOptions(
        beam_size=inference_config.beam_size,
        best_of=inference_config.best_of,
        patience=inference_config.patience,
        length_penalty=inference_config.length_penalty,
        repetition_penalty=inference_config.repetition_penalty,
        no_repeat_ngram_size=inference_config.no_repeat_ngram_size,
        log_prob_threshold=inference_config.log_prob_threshold,
        no_speech_threshold=inference_config.no_speech_threshold,
        compression_ratio_threshold=inference_config.compression_ratio_threshold,
        condition_on_previous_text=inference_config.condition_on_previous_text,
        prompt_reset_on_temperature=inference_config.prompt_reset_on_temperature,
        temperatures=inference_config.temperatures,
        initial_prompt=inference_config.initial_prompt,
        prefix=inference_config.prefix,
        suppress_blank=inference_config.suppress_blank,
        suppress_tokens=inference_config.suppress_tokens,
        without_timestamps=inference_config.without_timestamps,
        max_initial_timestamp=inference_config.max_initial_timestamp,
        word_timestamps=inference_config.word_timestamps,
        prepend_punctuations=inference_config.prepend_punctuations,
        append_punctuations=inference_config.append_punctuations,
        multilingual=inference_config.multilingual,
        max_new_tokens=inference_config.max_new_tokens,
        clip_timestamps=inference_config.clip_timestamps,
        hallucination_silence_threshold=inference_config.hallucination_silence_threshold,
        hotwords=inference_config.hotwords,
    )


@InferenceRunner.register(ASRModel.FASTER_WHISPER)
class FasterWhisperInferenceRunner(InferenceRunner):
    """Inference runner for faster-whisper large-v3-turbo.

    Transcribes multilingual audio. Expects 16 kHz mono wav files or tensors.
    Requires faster-whisper from https://github.com/SYSTRAN/faster-whisper.
    """

    def __init__(
        self,
        config: FasterWhisperInferenceRunnerConfig = None,
        device: TorchDevice = TorchDevice.CPU,
    ):
        super().__init__(device=device)
        if config is None:
            config = FasterWhisperInferenceRunnerConfig()
        self._config = config
        self._model = None

    @classmethod
    def _from_config(
        cls,
        config: FasterWhisperInferenceRunnerConfig,
        device: TorchDevice = TorchDevice.CPU,
        **extras,
    ) -> FromConfig:
        return cls(config=config, device=device, **extras)

    def __enter__(self):
        import faster_whisper

        self._model = faster_whisper.BatchedInferencePipeline(
            faster_whisper.WhisperModel(
                self._config.whisper_model_name,
                device=str(self._torch_device),
                compute_type=self._config.compute_type,
            )
        )
        return self

    def process(
        self,
        inputs: Iterable[tuple[ProcessedAudioSegment, ...]],
        *,
        languages: tuple[str] | None = None,
        **kwargs,
    ) -> Iterable[ASRResult | Error]:
        """Transcribe batches of preprocessed audio segments using
        faster_whisper.generate_segment_batched.

        :param inputs: batches of PreprocessorOutput (file-backed or tensor)
        :param languages: ISO-639-1 code or list of codes
        :return: ASRResult per input, in batch order
        """
        import numpy as np
        from faster_whisper.audio import (
            pad_or_trim,
        )
        from faster_whisper.tokenizer import (
            Tokenizer,
        )
        from faster_whisper.transcribe import (
            Segment,
        )

        options = inference_config_to_transcription_options(self._config)

        for idx, input_batch in enumerate(inputs):
            if len(input_batch) == 0:
                continue

            if isinstance(input_batch[0], MemoryProcessedSegment):
                tensors = [inp.tensor.detach().cpu().numpy() for inp in input_batch]
            elif (
                isinstance(input_batch[0], FSProcessedSegment)
                and input_batch[0].path is not None
            ):
                tensors = [load_audio(inp.path).numpy() for inp in input_batch]
            else:
                continue

            raw_features = [
                self._model.model.feature_extractor(tensor)[..., :-1]
                for tensor in tensors
            ]

            del tensors

            features = np.stack([pad_or_trim(f) for f in raw_features])
            segment_metadata = [
                {
                    "offset": 0.0,
                    "duration": inp.metadata.duration_s,
                    "segments": [
                        {
                            "start": 0,
                            "end": int(inp.metadata.duration_s * DEFAULT_SAMPLE_RATE),
                        }
                    ],
                }
                for inp in input_batch
            ]

            if languages is None:
                language = self._model.model.detect_language(features=raw_features[0])[
                    0
                ]
            else:
                language = languages[idx]

            tokenizer = Tokenizer(
                self._model.model.hf_tokenizer,
                self._model.model.model.is_multilingual,
                task="transcribe",
                language=language,
            )

            outputs = self._model.forward(
                features, tokenizer, segment_metadata, options
            )

            for inp, output in zip(input_batch, outputs, strict=False):
                segments = [
                    Segment(
                        id=seg_idx,
                        seek=out["seek"],
                        start=out["start"],
                        end=out["end"],
                        text=out["text"],
                        tokens=out["tokens"],
                        avg_logprob=out["avg_logprob"],
                        no_speech_prob=out["no_speech_prob"],
                        compression_ratio=out["compression_ratio"],
                        words=None,
                        temperature=options.temperatures[0],
                    )
                    for seg_idx, out in enumerate(output)
                ]
                yield ASRResult.from_faster_whisper_result(
                    segments, index=inp.metadata.index
                )
