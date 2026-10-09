import math
from collections import namedtuple
from pathlib import Path

import pytest
import torch
from caul.model_cache import cache_faster_whisper_models
from caul.tasks.inference.faster_whisper import FasterWhisperInferenceRunner
from caul_core import (
    DEFAULT_SAMPLE_RATE,
    ASRResult,
    FasterWhisperInferenceRunnerConfig,
    FSProcessedSegment,
    MemoryProcessedSegment,
    SegmentIndex,
    SegmentMetadata,
)
from faster_whisper import BatchedInferencePipeline
from faster_whisper.tokenizer import Tokenizer
from faster_whisper.transcribe import TranscriptionOptions
from huggingface_hub.constants import HF_HUB_CACHE
from numpy import ndarray
from torchcodec.encoders import AudioEncoder

EN_TEXT_A = "hello"
EN_TEXT_B = "world"

SEG_START_A, SEG_END_A, LOG_PROB_A = 0.0, 2.0, -0.2
SEG_START_B, SEG_END_B, LOG_PROB_B = 2.0, 5.0, -0.3


def _mock_raw_segment(
    start: float,
    end: float,
    text: str,
    avg_logprob: float,
    *,
    seek: int | None = None,
    tokens: list[int] | None = None,
    no_speech_prob: float = 0.5,
    compression_ratio: float = 0.5,
) -> dict:
    if tokens is None:
        tokens = []
    return {
        "start": start,
        "end": end,
        "seek": seek,
        "text": text,
        "avg_logprob": avg_logprob,
        "tokens": tokens,
        "no_speech_prob": no_speech_prob,
        "compression_ratio": compression_ratio,
    }


MOCK_RAW_SEGMENTS = [
    [_mock_raw_segment(SEG_START_A, SEG_END_A, EN_TEXT_A, LOG_PROB_A)],
    [_mock_raw_segment(SEG_START_B, SEG_END_B, EN_TEXT_B, LOG_PROB_B)],
]


_MockSegment = namedtuple("_MockSegment", ["start", "end", "text", "avg_logprob"])

MOCK_SEGMENTS = [
    _MockSegment(s[0]["start"], s[0]["end"], s[0]["text"], s[0]["avg_logprob"])
    for s in MOCK_RAW_SEGMENTS
]

_MOCK_OUTPUT_SEGMENTS = [
    _mock_raw_segment(SEG_START_A, SEG_END_A, EN_TEXT_A, LOG_PROB_A),
    _mock_raw_segment(SEG_START_B, SEG_END_B, EN_TEXT_B, LOG_PROB_B),
]


class MockWhisperModelModel:
    def __init__(self):
        self.is_multilingual = True


class MockHfTokenizer:
    def token_to_id(self, token: str) -> str:  # noqa: ARG002
        return "task"


class _MockWhisperModel:
    def __init__(self):
        self.hf_tokenizer = MockHfTokenizer()
        self.model = MockWhisperModelModel()

    # Mirrors the faster_whisper WhisperModel API
    def feature_extractor(
        self,
        waveform: ndarray,
        padding: int = 160,  # noqa: ARG002
        chunk_length: int | None = None,  # noqa: ARG002
    ) -> ndarray:
        return waveform

    def detect_language(self, features: ndarray) -> tuple[str, float]:  # noqa: ARG002
        return "en", 1.0


class _MockBatchedInferencePipeline:
    """Returns the same two-segment result for every input in the batch"""

    def __init__(self):
        self.model = _MockWhisperModel()

    # Mirrors the faster_whisper BatchedInferencePipeline.forward signature
    def forward(
        self,
        features: ndarray,  # noqa: ARG002
        tokenizer: Tokenizer,  # noqa: ARG002
        chunks_metadata: list[dict],
        options: TranscriptionOptions,  # noqa: ARG002
    ) -> list[list[dict]]:
        return [_MOCK_OUTPUT_SEGMENTS for _ in range(len(chunks_metadata))]


class MockFasterWhisperInferenceRunner(FasterWhisperInferenceRunner):
    def __enter__(self):
        self._model = _MockBatchedInferencePipeline()
        return self


def _fs_backed(
    root: Path,
    name: str = "audio.wav",
    index: SegmentIndex | None = None,
    duration_s: float = 2.0,
) -> FSProcessedSegment:
    if index is None:
        index = SegmentIndex()
    path = root / name
    audio = torch.zeros(1, int(duration_s * 16000))
    AudioEncoder(audio, sample_rate=DEFAULT_SAMPLE_RATE).to_file(path)
    return FSProcessedSegment(
        path=path, metadata=SegmentMetadata(duration_s=duration_s, index=index)
    )


def _memory_backed(
    index: SegmentIndex | None = None, duration_s: float = 2.0
) -> MemoryProcessedSegment:
    if index is None:
        index = SegmentIndex()
    return MemoryProcessedSegment(
        metadata=SegmentMetadata(duration_s=duration_s, index=index),
        tensor=torch.zeros(int(duration_s * 16000)),
    )


class TestASRResultFromFasterWhisper:
    def test__multiple_segments_build_transcription(self) -> None:
        result = ASRResult.from_faster_whisper_result(
            MOCK_SEGMENTS, index=SegmentIndex()
        )

        assert result.index == SegmentIndex()
        assert result.transcription == [
            (SEG_START_A, SEG_END_A, EN_TEXT_A),
            (SEG_START_B, SEG_END_B, EN_TEXT_B),
        ]

    def test__score_is_duration_weighted_avg_logprob(self) -> None:
        result = ASRResult.from_faster_whisper_result(
            MOCK_SEGMENTS, index=SegmentIndex()
        )

        dur_a = SEG_END_A - SEG_START_A
        dur_b = SEG_END_B - SEG_START_B
        expected = math.exp((LOG_PROB_A * dur_a + LOG_PROB_B * dur_b) / (dur_a + dur_b))
        assert result.score == pytest.approx(expected, abs=1e-9)

    def test__single_segment(self) -> None:
        seg = _MockSegment(1.0, 4.0, "only", -0.1)
        result = ASRResult.from_faster_whisper_result(
            [seg], index=SegmentIndex(audio=2)
        )

        assert result.transcription == [(1.0, 4.0, "only")]
        assert result.score == pytest.approx(math.exp(-0.1), abs=1e-9)

    def test__empty_segments_give_empty_transcription_and_unit_score(self) -> None:
        result = ASRResult.from_faster_whisper_result([], index=SegmentIndex())

        assert result.transcription == []
        assert result.score == -1.0


class TestFasterWhisperInferenceRunner:
    def setup_method(self) -> None:
        self._runner = MockFasterWhisperInferenceRunner()

    def test__yields_one_result_per_input_in_batch(self, tmp_path: Path) -> None:
        inputs = [
            _fs_backed(tmp_path, "a.wav", index=SegmentIndex(audio=0)),
            _fs_backed(tmp_path, "b.wav", index=SegmentIndex(audio=1)),
        ]
        with self._runner:
            results = list(self._runner.process([inputs]))

        assert len(results) == 2
        assert all(isinstance(r, ASRResult) for r in results)

    def test__preserves_index(self, tmp_path: Path) -> None:
        inputs = [
            _fs_backed(tmp_path, f"{i}.wav", index=SegmentIndex(audio=i))
            for i in range(3)
        ]
        with self._runner:
            results = list(self._runner.process([inputs]))
        expected = [SegmentIndex(audio=i) for i in range(3)]
        assert [r.index for r in results] == expected

    def test__results_contain_transcription(self, tmp_path: Path) -> None:
        inp = _fs_backed(tmp_path, index=SegmentIndex(audio=0))
        with self._runner:
            results = list(self._runner.process([[inp]]))

        assert results[0].transcription == [
            (SEG_START_A, SEG_END_A, EN_TEXT_A),
            (SEG_START_B, SEG_END_B, EN_TEXT_B),
        ]

    def test__skips_empty_batches(self) -> None:
        with self._runner:
            results = list(self._runner.process([[], []]))
        assert results == []

    def test__multiple_batches_each_yield_results(self, tmp_path: Path) -> None:
        batch_a = [_fs_backed(tmp_path, "a.wav", index=SegmentIndex(audio=0))]
        batch_b = [_fs_backed(tmp_path, "b.wav", index=SegmentIndex(audio=1))]
        with self._runner:
            results = list(self._runner.process([batch_a, batch_b]))

        assert len(results) == 2

    @pytest.mark.no_ci
    def test_inference_runner_should_cache_to_dir_and_load_from_it(self) -> None:
        # Given
        # Let's use the local cache to avoid downloading for ages
        cache_dir = HF_HUB_CACHE
        # When
        cache_faster_whisper_models(cache_dir)
        runner = FasterWhisperInferenceRunner.from_config(
            FasterWhisperInferenceRunnerConfig()
        )
        with runner:
            # Then
            assert isinstance(runner._model, BatchedInferencePipeline)
