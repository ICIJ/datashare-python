import logging
import uuid
from collections.abc import Iterable
from functools import partial
from hashlib import sha256
from itertools import repeat
from pathlib import Path
from typing import TYPE_CHECKING, Self, final

from caul_core import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_LARGE_FILE_THRESHOLD_BYTES,
    DEFAULT_MAX_FRAMES,
    DEFAULT_SAMPLE_RATE,
    ASRInput,
    Audio,
    AudioMetadata,
    BaseBatcherConfig,
    BasePreprocessorConfig,
    ConstantSizeBatcherConfig,
    Error,
    FSProcessedSegment,
    MemoryProcessedSegment,
    Preprocessor,
    ProcessedAudioSegment,
    SampleRate,
    SegmentIndex,
    SegmentMetadata,
)
from torch import Tensor

from caul.segmentation.methods import SegmentationFunction

from ...exception import UnreadableAudio
from ...segmentation import segment_by_silence
from ...utils import audio_decoder, load_audio, save_tensor
from .batcher import Batcher

if TYPE_CHECKING:
    import numpy as np
    import torch

_NoneType = type(None)

logger = logging.getLogger(__name__)


class ASRPreprocessorMixin(Preprocessor):
    """Preprocessing logic for ASR model inputs"""

    def __init__(
        self,
        *,
        batcher: BaseBatcherConfig | None = None,
        max_frames: int = DEFAULT_MAX_FRAMES,
        sample_rate: int = DEFAULT_SAMPLE_RATE,
        large_file_threshold_bytes: int = DEFAULT_LARGE_FILE_THRESHOLD_BYTES,
        segmentation_fn: SegmentationFunction = segment_by_silence,
        reported_errors: tuple[type[Exception]] | None = None,
    ):
        super().__init__()
        if batcher is None:
            batcher = ConstantSizeBatcherConfig(batch_size=DEFAULT_BATCH_SIZE)
        self._batcher_factory = partial(Batcher.from_config, config=batcher)
        self._max_frames = max_frames
        self._sample_rate = sample_rate
        self._large_file_threshold_bytes = large_file_threshold_bytes
        self._segmentation_fn = segmentation_fn
        if reported_errors is None:
            reported_errors = (UnreadableAudio,)
        self._reported_errors = reported_errors

    @classmethod
    def _from_config(cls, config: BasePreprocessorConfig, **extras) -> Self:
        # TODO: configure segmentation fn
        return cls(
            max_frames=config.max_frames,
            batcher=config.batcher,
            sample_rate=config.sample_rate,
            large_file_threshold_bytes=config.large_file_threshold_bytes,
        )

    def process(
        self,
        inputs: ASRInput,
        sample_rates: SampleRate | None = None,
        output_dir: Path | None = None,
        **kwargs,
    ) -> Iterable[tuple[ProcessedAudioSegment, ...] | Error]:
        """Segment and batch audio inputs

        :param inputs: List of np.ndarray or torch.Tensor or str, or singleton of same
            types
        :param sample_rates: sample rate(s) of audio inputs
        :param output_dir: optional directory to write preprocessed wav segments
        :return: batches of indexed preprocessed audio tensors
            (input_idx, preprocessed_input)
        """
        processed_segments = self.preprocess_inputs(
            inputs, sample_rates, output_dir=output_dir
        )
        batcher = self._batcher_factory(items=processed_segments)
        # Yield result first, we need to iterate on all processed segments to batch
        # only successful segments and collect failures. Order is not maintained
        # between successful results and errors which is fine as long order is
        # maintained for results which processed downstream
        yield from batcher.batch()

    @final
    def preprocess_inputs(
        self,
        inputs: ASRInput,
        input_sample_rates: SampleRate | None = None,
        output_dir: str | Path | None = None,
    ) -> Iterable[ProcessedAudioSegment | Error]:
        """Convert audio inputs to normalized and segmented torch.Tensor.

        Accepts audio inputs as a list of file paths, np.ndarray, or torch.Tensor,
        converting to torch.Tensor, normalizing, segmenting inputs longer than seg_max
        and batching segments

        :param inputs: List of np.ndarray or torch.Tensor or str, or a singleton of same
            types
        :param input_sample_rates: sample rate(s) of audio inputs
        :param output_dir: if provided, save segments as wav files here
        :return: List of processed inputs
        """
        if output_dir is not None and not isinstance(output_dir, Path):
            output_dir = Path(output_dir)

        if isinstance(input_sample_rates, (int, _NoneType)):
            input_sample_rates = repeat(input_sample_rates)
            inputs_and_sample_rates = zip(inputs, input_sample_rates, strict=False)
        else:
            inputs_and_sample_rates = zip(inputs, input_sample_rates, strict=True)
        seg_meta = None
        for audio_idx, (raw_input, input_sample_rate) in enumerate(
            inputs_and_sample_rates
        ):
            audio_input, audio_meta = self._audio_meta_from_input(audio_idx, raw_input)
            try:
                audio, sample_rate = self._load_audio(audio_input, input_sample_rate)
                segs = self._segment_audio(audio, sample_rate)
                for seg_position, (raw_audio_seg, seg_duration) in enumerate(segs):
                    seg_idx = SegmentIndex(audio=audio_meta.index, segment=seg_position)
                    seg_meta = SegmentMetadata(
                        index=seg_idx,
                        audio_format=audio_meta.audio_format,
                        audio_path=audio_meta.audio_path,
                        duration_s=seg_duration,
                    )
                    audio_seg = self._preprocess_segment(raw_audio_seg)
                    if output_dir is not None:
                        seg_path = _persist_seg(
                            audio_seg,
                            seg_idx.segment,
                            audio_meta.audio_path,
                            output_dir=output_dir,
                        ).relative_to(output_dir)
                        res = FSProcessedSegment(metadata=seg_meta.now(), path=seg_path)
                    else:
                        res = MemoryProcessedSegment(
                            metadata=seg_meta.now(), tensor=audio_seg
                        )
                    yield res
            # Catch expected audio processing errors, let the other stop the processing
            # (implem bug or unexpected errors which should be dealt with)
            except self._reported_errors as cause:
                meta = seg_meta or audio_meta
                msg = f"error while preprocessing segment {seg_meta}. Skipping !"
                logger.exception(msg)
                error = Error.from_exception(cause, meta)
                yield error

    def _audio_meta_from_input(
        self, audio_idx: int, audio_input: ASRInput
    ) -> tuple[Audio, AudioMetadata]:
        audio_id = audio_idx
        if isinstance(audio_input, tuple):
            audio_id, audio_input = audio_input
        audio_format, audio_path = None, None
        if isinstance(audio_input, str):
            audio_input = Path(audio_input)
        if isinstance(audio_input, Path):
            audio_path = audio_input
            audio_format = audio_input.suffix.removeprefix(".") or None
        audio_meta = AudioMetadata(
            index=audio_id, audio_format=audio_format, audio_path=audio_path
        )
        return audio_input, audio_meta

    def _segment_audio(
        self, audio_chunks: Iterable[Tensor], sample_rate: int
    ) -> Iterable[tuple[Tensor, float]]:
        for chunk in audio_chunks:
            n_frames = chunk.shape[-1]
            if n_frames <= self._max_frames:
                yield (chunk, n_frames / sample_rate)
                continue
            max_seg_len_s = self._max_frames / sample_rate
            for s in self._segmentation_fn(
                chunk, sample_rate=sample_rate, max_segment_len_s=max_seg_len_s
            ):
                yield (s.tensor, s.duration)

    def _load_audio(
        self, audio_input: "np.ndarray | torch.Tensor | Path", sample_rate: int | None
    ) -> tuple[Iterable[Tensor], int]:
        import numpy as np

        if isinstance(audio_input, (Path, str)):
            audio_path = Path(audio_input)
            sample_rate = self._sample_rate
            audio_chunks = self._load_file_as_chunks(audio_path, sample_rate)
        else:
            if isinstance(audio_input, np.ndarray):
                import torch

                audio_input = torch.Tensor(audio_input)
            if sample_rate is None:
                sample_rate = self._sample_rate
                audio_input = self._normalize(audio_input, sample_rate)
            elif len(audio_input.shape) > 1:
                audio_input = audio_input.squeeze(0)
            audio_chunks = iter([audio_input])
        return audio_chunks, sample_rate

    def _preprocess_segment(self, audio_tensor: "torch.Tensor") -> "torch.Tensor":
        """Stub for subclasses that have specific audio preprocessing logic"""
        return audio_tensor

    def _load_file_as_chunks(
        self, path: Path, sample_rate: int
    ) -> "Iterable[torch.Tensor]":
        """Return a lazy iterator of normalized 1D audio chunks at self._sample_rate.

        Reads file metadata first; falls back to a single eager load for small files.

        :param path: path to audio file
        :return: Iterable of tensor segments
        """
        try:
            native_meta = audio_decoder(path).metadata
        except ValueError as e:
            raise UnreadableAudio(path) from e
        num_frames = int(native_meta.duration_seconds * native_meta.sample_rate)
        estimated_bytes = num_frames * native_meta.num_channels * 4  # float32

        try:
            if estimated_bytes > self._large_file_threshold_bytes:
                yield from self._iter_audio_chunks(path, native_meta.duration_seconds)
            else:
                yield load_audio(path, sample_rate=sample_rate, num_channels=1)
        except Exception as e:
            raise UnreadableAudio(path) from e

    def _iter_audio_chunks(
        self, path: Path, total_duration_s: float
    ) -> "Iterable[torch.Tensor]":
        """Lazily decode a large audio file in max-frame-sized windows.

        :param path: path to audio file
        :param total_duration_s: total duration of audio file in seconds
        :return: Iterable of tensor segments
        """
        chunk_duration_s = self._max_frames / self._sample_rate
        decoder = audio_decoder(path, self._sample_rate, num_channels=1)
        start = 0.0
        while start < total_duration_s:
            end = min(start + chunk_duration_s, total_duration_s)
            samples = decoder.get_samples_played_in_range(
                start_seconds=start, stop_seconds=end
            )
            yield samples.data.squeeze()
            start = end

    def _normalize(
        self, audio_tensor: "torch.Tensor", sample_rate: int
    ) -> "torch.Tensor":
        """Normalize audio_tensor (single channel, sample rate = 16000)

        :param audio_tensor: input tensor
        :param sample_rate: input sample rate
        :return: normalized 1D tensor at self._sample_rate
        """
        if sample_rate != self._sample_rate:
            audio_tensor = _resample_audio(
                audio_tensor, self._sample_rate, target_rate=sample_rate
            )

        # Stereo dims (channels, aud_length); need mono (aud_length)
        if len(audio_tensor.shape) > 1:
            audio_tensor = audio_tensor.squeeze(0)

        return audio_tensor


def _persist_seg(
    audio_seg: Tensor, index: int, audio_path: Path | None, *, output_dir: Path
) -> Path:
    original_file = (
        _displayable_prefix(audio_path) if audio_path is not None else uuid.uuid4().hex
    )
    seg_name = f"{original_file}-{index}.wav"
    seg_path = output_dir / seg_name
    save_tensor(audio_seg, seg_path)
    return seg_path


def _resample_audio(
    audio: "torch.Tensor", sample_rate: int, *, target_rate: int
) -> "torch.Tensor":
    from torchcodec.encoders import AudioEncoder

    encoder = AudioEncoder(samples=audio, sample_rate=sample_rate)

    return encoder.to_tensor(format="wav", num_channels=1, sample_rate=target_rate)


def _displayable_prefix(
    path: Path, component_size_limit: int = 10, *, deterministic: bool = False
) -> str:
    displayable_file_name = path.name[:component_size_limit].replace(".", "__")
    if deterministic:
        uid = sha256(str(path).encode()).hexdigest()[:20]
    else:
        uid = uuid.uuid4().hex[:20]
    return f"{displayable_file_name}-{uid}"
