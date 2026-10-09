from collections.abc import Callable
from unittest.mock import MagicMock

import pytest
import torch
from caul.segmentation import (
    FixedSegmentationConfig,
    PyannoteVoiceSegmentationConfig,
    SegmentationConfig,
    SilenceSegmentationConfig,
    SileroVoiceSegmentationConfig,
    TensorSegment,
    segment_by_pyannote_vad,
    segment_by_silence,
    segment_by_silero_vad,
    segment_fixed,
)
from caul.segmentation.segmenter import (
    AudioSegmenter,
    PyannoteVoiceAudioSegmenter,
    SileroVoiceAudioSegmenter,
)
from caul_core import DEFAULT_SAMPLE_RATE

from tests.unit.constant import TEST_TIMESTAMPS


def make_silent_tensor(
    duration_secs: float, sample_rate: int = DEFAULT_SAMPLE_RATE
) -> "torch.Tensor":
    """Zero tensor treated as silence by librosa.effects.split"""
    return torch.zeros(int(duration_secs * sample_rate))


def make_noisy_tensor(
    duration_secs: float, sample_rate: int = DEFAULT_SAMPLE_RATE
) -> "torch.Tensor":
    """Noisy tensor with uniform random noise"""
    return torch.rand(int(duration_secs * sample_rate))


def make_voiced_tensor(
    voice_durations_secs: list[float],
    silence_between_secs: float = 1.5,
    sample_rate: int = DEFAULT_SAMPLE_RATE,
) -> "torch.Tensor":
    """Build a tensor alternating noise and silent regions"""
    chunks = []
    for i, duration in enumerate(voice_durations_secs):
        chunks.append(torch.rand(int(duration * sample_rate)))
        if i < len(voice_durations_secs) - 1:
            chunks.append(torch.zeros(int(silence_between_secs * sample_rate)))
    return torch.cat(chunks)


def _make_silero_mock(speech_timestamps: list[dict]) -> tuple[MagicMock, MagicMock]:
    vad_model = MagicMock()
    vad_parser_fn = MagicMock(return_value=speech_timestamps)
    return vad_model, vad_parser_fn


def _make_pyannote_mock(speech_intervals_s: list[tuple[float, float]]) -> MagicMock:
    """Create a mock pyannote pipeline returning the given speech intervals (in s)"""

    class _Seg:
        def __init__(self, start: float, end: float):
            self.start = start
            self.end = end

    segs = [_Seg(s, e) for s, e in speech_intervals_s]
    timeline = MagicMock()
    timeline.support.return_value = segs
    annotation = MagicMock()
    annotation.get_timeline.return_value = timeline
    pipeline = MagicMock()
    pipeline.return_value = annotation
    return pipeline


# TensorSegment


class TestTensorSegment:
    def test__duration_computed_correctly(self) -> None:

        seg = TensorSegment(
            tensor=torch.zeros(16000),
            segment_start=0,
            segment_end=16000,
            sample_rate=16000,
            tensor_id="abc",
        )
        assert seg.duration == 1

    def test__lower_sample_rate(self) -> None:

        seg = TensorSegment(
            tensor=torch.zeros(8000),
            segment_start=0,
            segment_end=8000,
            sample_rate=8000,
            tensor_id="abc",
        )
        assert seg.duration == 1


# segment_fixed


class TestSegmentFixed:
    def test__single_chunk_shorter_than_segment_duration(self) -> None:
        tensor = make_silent_tensor(5.0)
        segments = segment_fixed(tensor, max_segment_len_s=10.0)
        assert len(segments) == 1
        assert segments[0].segment_start == 0
        assert segments[0].segment_end == tensor.shape[-1]

    def test__exact_multiple_produces_correct_num_segments(self) -> None:
        tensor = make_silent_tensor(10.0)
        segments = segment_fixed(tensor, max_segment_len_s=5.0)
        assert len(segments) == 2

    def test__inexact_includes_remainder(self) -> None:
        tensor = make_silent_tensor(11.0)
        segments = segment_fixed(tensor, max_segment_len_s=5.0)
        assert len(segments) == 3

    def test__segments_are_contiguous_and_non_overlapping(self) -> None:
        tensor = make_silent_tensor(13.0)
        segments = segment_fixed(tensor, max_segment_len_s=5.0)
        for i in range(1, len(segments)):
            assert segments[i].segment_start == segments[i - 1].segment_end

    def test__tensor_content_matches_slice(self) -> None:
        tensor = torch.arange(float(DEFAULT_SAMPLE_RATE * 3))
        segments = segment_fixed(tensor, max_segment_len_s=1.0)
        for seg in segments:
            assert torch.equal(seg.tensor, tensor[seg.segment_start : seg.segment_end])

    def test__all_segments_share_tensor_id(self) -> None:
        segments = segment_fixed(make_silent_tensor(6.0), max_segment_len_s=2.0)
        tensor_ids = {s.tensor_id for s in segments}
        assert len(tensor_ids) == 1

    def test__separate_calls_produce_different_tensor_ids(self) -> None:
        tensor = make_silent_tensor(3.0)
        segments_a = segment_fixed(tensor, max_segment_len_s=1.0)
        segments_b = segment_fixed(tensor, max_segment_len_s=1.0)
        assert segments_a[0].tensor_id != segments_b[0].tensor_id


# segment_by_silence


class TestSegmentBySilence:
    def test__all_silent_returns_single_segment(self) -> None:
        """librosa.effects.split treats an all-zero tensor as one interval spanning
        the full tensor rather than as pure silence, so we get one segment back
        """
        tensor = make_silent_tensor(2.0)
        segments = segment_by_silence(tensor)
        assert len(segments) == 1
        assert segments[0].segment_start == 0
        assert segments[0].segment_end == tensor.shape[-1]

    def test__segments_do_not_exceed_max_length(self) -> None:
        tensor = make_noisy_tensor(5.0)
        max_secs = 2.0
        segments = segment_by_silence(tensor, max_segment_len_s=max_secs)
        for seg in segments:
            assert seg.duration <= max_secs

    def test__oversized_interval_splits_correctly(self) -> None:
        tensor = make_noisy_tensor(6.0)
        segments = segment_by_silence(tensor, max_segment_len_s=2.0)
        assert len(segments) >= 3

    def test__all_segments_share_tensor_id(self) -> None:
        tensor = make_noisy_tensor(6.0)
        segments = segment_by_silence(tensor, max_segment_len_s=2.0)
        tensor_ids = {s.tensor_id for s in segments}
        assert len(tensor_ids) == 1

    def test__separate_calls_produce_different_tensor_ids(self) -> None:
        tensor = make_noisy_tensor(2.0)
        segments_a = segment_by_silence(tensor)
        segments_b = segment_by_silence(tensor)
        assert segments_a[0].tensor_id != segments_b[0].tensor_id

    def test__splits_voiced_tensor_into_one_segment_per_voice(self) -> None:
        voice_durations = [1.0, 1.5, 0.8]
        tensor = make_voiced_tensor(voice_durations)
        segments = segment_by_silence(tensor)
        assert len(segments) == len(voice_durations)

    def test__segment_durations_approximate_voice_regions(self) -> None:
        voice_durations = [1.0, 1.5, 0.8]
        silence_secs = 1.0
        tensor = make_voiced_tensor(voice_durations, silence_between_secs=silence_secs)
        segments = segment_by_silence(tensor)
        for seg, expected_secs in zip(segments, voice_durations, strict=True):
            assert seg.duration == pytest.approx(expected_secs, abs=silence_secs)


class TestSegmentBySileroVad:
    def test__returns_tensor_segment_per_timestamp(self) -> None:
        timestamps = [{"start": 0, "end": 8000}, {"start": 16000, "end": 24000}]
        model, vad_parser_fn = _make_silero_mock(timestamps)
        segments = segment_by_silero_vad(
            make_silent_tensor(2.0), vad_model=model, vad_parser_fn=vad_parser_fn
        )

        assert len(segments) == 2
        assert segments[0].segment_start == 0
        assert segments[0].segment_end == 8000
        assert segments[1].segment_start == 16000
        assert segments[1].segment_end == 24000

    def test__no_speech_returns_empty(self) -> None:
        model, vad_parser_fn = _make_silero_mock([])
        segments = segment_by_silero_vad(
            make_silent_tensor(1.0), vad_model=model, vad_parser_fn=vad_parser_fn
        )
        assert segments == []

    def test__config_params_forwarded_to_model(self) -> None:
        timestamps = [{"start": 0, "end": 4000}]
        model, vad_parser_fn = _make_silero_mock(timestamps)
        segment_by_silero_vad(
            make_silent_tensor(1.0),
            vad_model=model,
            vad_parser_fn=vad_parser_fn,
            sample_rate=8000,
            threshold=0.7,
            min_speech_duration_ms=500,
            min_silence_duration_ms=200,
            speech_pad_ms=50,
        )
        call_kwargs = vad_parser_fn.call_args.kwargs
        assert call_kwargs["sampling_rate"] == 8000
        assert call_kwargs["threshold"] == 0.7
        assert call_kwargs["min_speech_duration_ms"] == 500
        assert call_kwargs["min_silence_duration_ms"] == 200
        assert call_kwargs["speech_pad_ms"] == 50

    def test__oversized_segment_is_split(self) -> None:
        # One 4-second timestamp with a 2-second cap should produce 2 segments
        timestamps = [{"start": 0, "end": DEFAULT_SAMPLE_RATE * 4}]
        model, vad_parser_fn = _make_silero_mock(timestamps)
        segments = segment_by_silero_vad(
            make_silent_tensor(4.0),
            vad_model=model,
            vad_parser_fn=vad_parser_fn,
            max_segment_len_s=2.0,
        )
        assert len(segments) == 2
        assert segments[0].segment_end == DEFAULT_SAMPLE_RATE * 2
        assert segments[1].segment_start == DEFAULT_SAMPLE_RATE * 2

    def test__segments_do_not_exceed_max_length(self) -> None:
        timestamps = [{"start": 0, "end": DEFAULT_SAMPLE_RATE * 10}]
        model, vad_parser_fn = _make_silero_mock(timestamps)
        segments = segment_by_silero_vad(
            make_silent_tensor(10.0),
            vad_model=model,
            vad_parser_fn=vad_parser_fn,
            max_segment_len_s=3.0,
        )
        for seg in segments:
            assert seg.duration <= 3.0

    def test__all_segments_share_tensor_id(self) -> None:
        timestamps = [{"start": 0, "end": 8000}, {"start": 16000, "end": 24000}]
        model, vad_parser_fn = _make_silero_mock(timestamps)
        segments = segment_by_silero_vad(
            make_silent_tensor(2.0), vad_model=model, vad_parser_fn=vad_parser_fn
        )
        tensor_ids = {s.tensor_id for s in segments}
        assert len(tensor_ids) == 1

    def test__separate_calls_produce_different_tensor_ids(self) -> None:
        timestamps = [{"start": 0, "end": 8000}]
        model, vad_parser_fn = _make_silero_mock(timestamps)
        tensor = make_silent_tensor(1.0)
        segments_a = segment_by_silero_vad(
            tensor, vad_model=model, vad_parser_fn=vad_parser_fn
        )
        segments_b = segment_by_silero_vad(
            tensor, vad_model=model, vad_parser_fn=vad_parser_fn
        )
        assert segments_a[0].tensor_id != segments_b[0].tensor_id

    def test__segments_match_voiced_regions(self) -> None:
        """Build a tensor with three voiced regions and derive sample-accurate
        timestamps
        """
        voice_durations = [1.0, 1.5, 0.8]
        silence_secs = 0.6
        tensor = make_voiced_tensor(voice_durations, silence_between_secs=silence_secs)

        silence_samples = int(silence_secs * DEFAULT_SAMPLE_RATE)
        timestamps = []
        cursor = 0
        for duration in voice_durations:
            voice_samples = int(duration * DEFAULT_SAMPLE_RATE)
            timestamps.append({"start": cursor, "end": cursor + voice_samples})
            cursor += voice_samples + silence_samples

        model, vad_parser_fn = _make_silero_mock(timestamps)
        segments = segment_by_silero_vad(
            tensor, vad_model=model, vad_parser_fn=vad_parser_fn
        )

        assert len(segments) == len(voice_durations)
        for seg, ts in zip(segments, timestamps, strict=True):
            assert seg.segment_start == ts["start"]
            assert seg.segment_end == ts["end"]
            assert torch.equal(seg.tensor, tensor[ts["start"] : ts["end"]])


class TestSegmentByPyannoteVad:
    def test__returns_tensor_segment_per_speech_interval(self) -> None:
        intervals_s = [(0.0, 0.5), (1.0, 1.5)]
        pipeline = _make_pyannote_mock(intervals_s)
        tensor = make_silent_tensor(2.0)
        segments = segment_by_pyannote_vad(tensor, pipeline=pipeline)

        assert len(segments) == 2
        assert segments[0].segment_start == 0
        assert segments[0].segment_end == int(0.5 * DEFAULT_SAMPLE_RATE)
        assert segments[1].segment_start == int(1.0 * DEFAULT_SAMPLE_RATE)
        assert segments[1].segment_end == int(1.5 * DEFAULT_SAMPLE_RATE)

    def test__no_speech_returns_empty(self) -> None:
        pipeline = _make_pyannote_mock([])
        segments = segment_by_pyannote_vad(make_silent_tensor(1.0), pipeline=pipeline)
        assert segments == []

    def test__pipeline_instantiate_called_with_correct_params(self) -> None:
        pipeline = _make_pyannote_mock([(0.0, 0.5)])
        segment_by_pyannote_vad(
            make_silent_tensor(1.0),
            pipeline=pipeline,
            min_speech_duration_ms=200,
            min_silence_duration_ms=150,
        )
        call_args = pipeline.instantiate.call_args[0][0]
        assert call_args["min_duration_on"] == pytest.approx(0.2)
        assert call_args["min_duration_off"] == pytest.approx(0.15)

    def test__oversized_segment_is_split(self) -> None:
        # One 4-second interval with a 2-second cap should produce 2 segments
        pipeline = _make_pyannote_mock([(0.0, 4.0)])
        tensor = make_silent_tensor(4.0)
        segments = segment_by_pyannote_vad(
            tensor, pipeline=pipeline, max_segment_len_s=2.0
        )
        assert len(segments) == 2
        assert segments[0].segment_end == DEFAULT_SAMPLE_RATE * 2
        assert segments[1].segment_start == DEFAULT_SAMPLE_RATE * 2

    def test__segments_do_not_exceed_max_length(self) -> None:
        pipeline = _make_pyannote_mock([(0.0, 10.0)])
        segments = segment_by_pyannote_vad(
            make_silent_tensor(10.0), pipeline=pipeline, max_segment_len_s=3.0
        )
        for seg in segments:
            assert seg.duration <= 3.0

    def test__all_segments_share_tensor_id(self) -> None:
        pipeline = _make_pyannote_mock([(0.0, 0.5), (1.0, 1.5)])
        segments = segment_by_pyannote_vad(make_silent_tensor(2.0), pipeline=pipeline)
        tensor_ids = {s.tensor_id for s in segments}
        assert len(tensor_ids) == 1

    def test__separate_calls_produce_different_tensor_ids(self) -> None:
        tensor = make_silent_tensor(1.0)
        segments_a = segment_by_pyannote_vad(
            tensor, pipeline=_make_pyannote_mock([(0.0, 0.5)])
        )
        segments_b = segment_by_pyannote_vad(
            tensor, pipeline=_make_pyannote_mock([(0.0, 0.5)])
        )
        assert segments_a[0].tensor_id != segments_b[0].tensor_id

    def test__tensor_content_matches_slice(self) -> None:
        intervals_s = [(0.0, 0.5), (1.0, 1.5)]
        pipeline = _make_pyannote_mock(intervals_s)
        tensor = torch.arange(float(DEFAULT_SAMPLE_RATE * 2))
        segments = segment_by_pyannote_vad(tensor, pipeline=pipeline)

        for seg, (start_s, end_s) in zip(segments, intervals_s, strict=True):
            start_sample = int(start_s * DEFAULT_SAMPLE_RATE)
            end_sample = int(end_s * DEFAULT_SAMPLE_RATE)
            assert torch.equal(seg.tensor, tensor[start_sample:end_sample])


@AudioSegmenter.register("mock_silero")
class _MockSileroVADSegmenter(SileroVoiceAudioSegmenter):
    def __init__(
        self, config: SegmentationConfig, timestamps: list[dict] = TEST_TIMESTAMPS
    ) -> None:
        super().__init__(config)
        self._timestamps = timestamps

    def _load_vad_model(self) -> tuple["torch.nn.Module", Callable]:
        return _make_silero_mock(self._timestamps)


@AudioSegmenter.register("mock_pyannote")
class _MockPyannoteSegmenter(PyannoteVoiceAudioSegmenter):
    def __init__(
        self,
        config: PyannoteVoiceSegmentationConfig,
        intervals_s: list[tuple[float, float]] | None = None,
    ) -> None:
        super().__init__(config, hf_token="")
        self._intervals_s = intervals_s or []

    def _load_pipeline(self) -> MagicMock:
        return _make_pyannote_mock(self._intervals_s)


class TestAudioSegmenter:
    def test__dispatches_to_segment_fixed(self) -> None:
        segmenter = AudioSegmenter.from_config(
            FixedSegmentationConfig(max_segment_len_s=2.0)
        )
        with segmenter:
            segments = segmenter.segment(make_silent_tensor(5.0))

        assert len(segments) == 3

    def test__dispatches_to_segment_by_silence(self) -> None:
        segmenter = AudioSegmenter.from_config(SilenceSegmentationConfig())
        tensor = make_voiced_tensor([1.0, 1.0])
        with segmenter:
            segments = segmenter.segment(tensor)
            assert len(segments) == 2

    def test__dispatches_to_segment_by_silero_vad(self) -> None:
        voice_durations = [1.0, 1.5, 0.8]
        silence_secs = 0.6
        tensor = make_voiced_tensor(voice_durations, silence_between_secs=silence_secs)

        silence_samples = int(silence_secs * DEFAULT_SAMPLE_RATE)
        timestamps = []
        cursor = 0
        for duration in voice_durations:
            voice_samples = int(duration * DEFAULT_SAMPLE_RATE)
            timestamps.append({"start": cursor, "end": cursor + voice_samples})
            cursor += voice_samples + silence_samples

        segmenter = _MockSileroVADSegmenter(SileroVoiceSegmentationConfig(), timestamps)
        with segmenter:
            segments = segmenter.segment(tensor)

        assert len(segments) == 3

    def test__dispatches_to_segment_by_pyannote_vad(self) -> None:
        intervals_s = [(0.0, 1.0), (2.0, 3.5), (4.0, 4.8)]
        tensor = make_silent_tensor(5.0)
        segmenter = _MockPyannoteSegmenter(
            PyannoteVoiceSegmentationConfig(), intervals_s
        )
        with segmenter:
            segments = segmenter.segment(tensor)
        assert len(segments) == 3

    def test__context_manager_clears_vad_model_on_exit(self) -> None:
        segmenter = _MockSileroVADSegmenter(SileroVoiceSegmentationConfig())
        with segmenter:
            segmenter.segment(make_silent_tensor(1.0))
            assert segmenter._vad_model is not None
            assert segmenter._vad_parser_fn is not None
        assert segmenter._vad_model is None
        assert segmenter._vad_parser_fn is None
