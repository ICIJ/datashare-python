import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

from caul_core import DEFAULT_SAMPLE_RATE, PARAKEET_INFERENCE_MAX_DURATION_S

from .objects import TensorSegment

if TYPE_CHECKING:
    import pyannote
    import torch


class SegmentationFunction(Protocol):
    def __call__(
        self,
        audio_tensor: "torch.Tensor",
        *args,
        sample_rate: int,
        max_segment_len_s: float,
        **kwargs,
    ) -> list[TensorSegment]: ...


def _split_range_fixed(
    audio_tensor: "torch.Tensor",
    *,
    seg_start: int,
    seg_end: int,
    chunk_samples: int,
    sample_rate: int,
    tensor_id: str,
) -> list[TensorSegment]:
    """Split the range [seg_start, seg_end) into fixed-size chunks, with a
    remainder chunk at the end if the range is not an exact multiple.

    :param audio_tensor: 1D source tensor
    :param seg_start: start sample index (inclusive)
    :param seg_end: end sample index (exclusive)
    :param chunk_samples: maximum number of samples per chunk
    :param sample_rate: sample rate of the audio
    :param tensor_id: shared identifier linking all chunks to the source tensor
    :return: list of TensorSegment
    """
    segments = []
    while seg_start < seg_end:
        chunk_end = min(seg_start + chunk_samples, seg_end)
        segments.append(
            TensorSegment(
                tensor=audio_tensor[seg_start:chunk_end],
                segment_start=seg_start,
                segment_end=chunk_end,
                sample_rate=sample_rate,
                tensor_id=tensor_id,
            )
        )
        seg_start = chunk_end
    return segments


def segment_fixed(
    audio_tensor: "torch.Tensor",
    *,
    sample_rate: int = DEFAULT_SAMPLE_RATE,
    max_segment_len_s: float = PARAKEET_INFERENCE_MAX_DURATION_S,
) -> list[TensorSegment]:
    """Split an audio tensor into fixed-length chunks.

    :param audio_tensor: 1D input tensor
    :param sample_rate: sample rate of the audio
    :param max_segment_len_s: duration of each segment in seconds
    :return: list of TensorSegment
    """
    tensor_id = uuid.uuid4().hex
    chunk_samples = int(max_segment_len_s * sample_rate)
    return _split_range_fixed(
        audio_tensor,
        seg_start=0,
        seg_end=audio_tensor.shape[-1],
        chunk_samples=chunk_samples,
        sample_rate=sample_rate,
        tensor_id=tensor_id,
    )


def segment_by_silence(
    audio_tensor: "torch.Tensor",
    *,
    sample_rate: int = DEFAULT_SAMPLE_RATE,
    frame_len: int = 2048,
    silence_thresh_db: int = 35,
    hop_len: int = 512,
    kept_silence_len_s: float = 0.15,
    min_silence_len_s: float = 0.5,
    max_segment_len_s: float = PARAKEET_INFERENCE_MAX_DURATION_S,
) -> list[TensorSegment]:
    """Split an audio tensor on silences using librosa, falling back to fixed splits
    where merged intervals exceed the maximum segment length.

    :param audio_tensor: 1D input tensor
    :param sample_rate: sample rate of the audio
    :param frame_len: number of samples per librosa analysis frame
    :param silence_thresh_db: dB threshold below which audio is considered silent
    :param hop_len: number of samples between analysis frames
    :param kept_silence_len_s: seconds of silence to retain at segment boundaries
    :param min_silence_len_s: minimum silence duration to split on
    :param max_segment_len_s: maximum segment duration in seconds; segments
        exceeding this are split at fixed intervals as a fallback
    :return: list of TensorSegment
    """
    import librosa

    if len(audio_tensor.shape) != 1:
        msg = (
            f"expected 1D input tensor, got {audio_tensor} of shape"
            f" {len(audio_tensor.shape)}"
        )
        raise ValueError(msg)

    tensor_id = uuid.uuid4().hex

    nonsilent_intervals = librosa.effects.split(
        audio_tensor.numpy(),
        top_db=silence_thresh_db,
        frame_length=frame_len,
        hop_length=hop_len,
    )

    min_silence_samples = int(min_silence_len_s * sample_rate)
    kept_silence_samples = int(kept_silence_len_s * sample_rate)
    max_segment_samples = int(max_segment_len_s * sample_rate)

    # Merge intervals separated by silences shorter than the minimum
    merged: list[tuple[int, int]] = []
    for start, end in nonsilent_intervals:
        if len(merged) == 0:
            merged.append((start, end))
        else:
            prev_end = merged[-1][1]
            if start - prev_end < min_silence_samples:
                merged[-1] = (merged[-1][0], end)
            else:
                merged.append((start, end))

    total_samples = audio_tensor.shape[-1]
    segments = []

    # Fallback for segments over max_segment_len_secs
    for interval_start, interval_end in merged:
        seg_start = max(0, interval_start - kept_silence_samples)
        seg_end = min(total_samples, interval_end + kept_silence_samples)
        segments.extend(
            _split_range_fixed(
                audio_tensor,
                seg_start=seg_start,
                seg_end=seg_end,
                chunk_samples=max_segment_samples,
                sample_rate=sample_rate,
                tensor_id=tensor_id,
            )
        )

    return segments


def segment_by_silero_vad(
    audio_tensor: "torch.Tensor",
    vad_model: "torch.nn.Module",
    vad_parser_fn: Callable,
    *,
    sample_rate: int = DEFAULT_SAMPLE_RATE,
    threshold: float = 0.5,
    min_speech_duration_ms: int = 250,
    min_silence_duration_ms: int = 100,
    speech_pad_ms: int = 30,
    max_segment_len_s: float = PARAKEET_INFERENCE_MAX_DURATION_S,
) -> list[TensorSegment]:
    """Split an audio tensor into voiced segments using silero VAD.

    :param audio_tensor: 1D input tensor
    :param vad_model: silero VAD model
    :param vad_parser_fn: silero VAD segmentation function
    :param sample_rate: sample rate of the audio; silero-VAD supports 8000 or 16000
    :param threshold: speech probability threshold [0, 1)
    :param min_speech_duration_ms: minimum speech segment duration in ms
    :param min_silence_duration_ms: minimum silence duration to split on in ms
    :param speech_pad_ms: padding added around each speech segment in ms
    :param max_segment_len_s: maximum segment duration in seconds; segments
        exceeding this are split at fixed intervals as a fallback
    :return: list of TensorSegment
    """
    tensor_id = uuid.uuid4().hex
    max_segment_samples = int(max_segment_len_s * sample_rate)

    speech_timestamps = vad_parser_fn(
        audio_tensor,
        vad_model,
        sampling_rate=sample_rate,
        threshold=threshold,
        min_speech_duration_ms=min_speech_duration_ms,
        min_silence_duration_ms=min_silence_duration_ms,
        speech_pad_ms=speech_pad_ms,
        return_seconds=False,
    )

    # Fallback for segments over max_segment_len_secs
    segments = []
    for ts in speech_timestamps:
        segments.extend(
            _split_range_fixed(
                audio_tensor,
                seg_start=ts["start"],
                seg_end=ts["end"],
                chunk_samples=max_segment_samples,
                sample_rate=sample_rate,
                tensor_id=tensor_id,
            )
        )
    return segments


def segment_by_pyannote_vad(
    audio_tensor: "torch.Tensor",
    pipeline: "pyannote.audio.Pipeline",
    *,
    sample_rate: int = DEFAULT_SAMPLE_RATE,
    min_speech_duration_ms: int = 100,
    min_silence_duration_ms: int = 100,
    max_segment_len_s: float = PARAKEET_INFERENCE_MAX_DURATION_S,
) -> list[TensorSegment]:
    """Split an audio tensor into voiced segments using pyannote VAD.

    :param audio_tensor: 1D input tensor
    :param pipeline: pyannote voice activity detection pipeline
    :param sample_rate: sample rate of the audio
    :param min_speech_duration_ms: minimum speech segment duration in ms
    :param min_silence_duration_ms: minimum silence duration to split on in ms
    :param max_segment_len_s: maximum segment duration in seconds; segments
        exceeding this are split at fixed intervals as a fallback
    :return: list of TensorSegment
    """
    from torch import float32

    tensor_id = uuid.uuid4().hex
    max_segment_samples = int(max_segment_len_s * sample_rate)

    # pyannote expects seconds
    params = {
        "min_duration_on": min_speech_duration_ms / 1000,
        "min_duration_off": min_silence_duration_ms / 1000,
    }
    pipeline.instantiate(params)

    # pyannote expects a 2D tensor (channels x samples)
    waveform = audio_tensor.unsqueeze(0).to(float32)
    output = pipeline({"waveform": waveform, "sample_rate": sample_rate})

    # Fallback for segments over max_segment_len_s
    segments = []
    for segment in output.get_timeline().support():
        start_sample = int(segment.start * sample_rate)
        end_sample = int(segment.end * sample_rate)
        segments.extend(
            _split_range_fixed(
                audio_tensor,
                seg_start=start_sample,
                seg_end=end_sample,
                chunk_samples=max_segment_samples,
                sample_rate=sample_rate,
                tensor_id=tensor_id,
            )
        )
    return segments
