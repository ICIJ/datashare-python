from pathlib import Path

import pytest
import torch
from caul.model_cache import cache_fireredasr2_models
from caul.tasks import FireRedASR2InferenceRunner, FireRedASR2Preprocessor
from caul_core import (
    FIREREDASR2_INFERENCE_MAX_FRAMES,
    ASRResult,
    ConstantSizeBatcherConfig,
    FireRedASR2InferenceRunnerConfig,
    SegmentIndex,
)
from fireredasr2s.fireredasr2 import FireRedAsr2
from huggingface_hub.constants import HF_HUB_CACHE

ZH_TEXT = "你好世界"
ZH_CONFIDENCE = 0.97
ZH_TIMESTAMP = [("你", 0.1, 0.4), ("好", 0.4, 0.7), ("世", 0.7, 1.0), ("界", 1.0, 1.3)]


def _make_result(uttid: str, text: str = ZH_TEXT, dur_s: float = 2.0) -> dict:
    return {
        "uttid": uttid,
        "text": text,
        "confidence": ZH_CONFIDENCE,
        "dur_s": dur_s,
        "rtf": "0.05",
        "wav": "dummy.wav",
        "timestamp": ZH_TIMESTAMP,
    }


class MockFireRedAsr2:
    # Mirrors the FireRedAsr2.transcribe signature
    def transcribe(
        self,
        batch_uttid: list[str],
        batch_wav_path: list[str],  # noqa: ARG002
    ) -> list[dict]:
        return [_make_result(uid) for uid in batch_uttid]


class MockFireRedASR2InferenceRunner(FireRedASR2InferenceRunner):
    def __enter__(self):
        self._model = MockFireRedAsr2()
        return self


# ASRResult.from_fireredasr2_result


class TestASRResultFromFireRedASR2:
    def test__result_with_timestamps(self) -> None:
        result = _make_result("utt0")
        asr = ASRResult.from_fireredasr2_result(result, index=SegmentIndex(audio=0))

        assert asr.index == SegmentIndex(audio=0)
        assert len(asr.transcription) == 1
        start, end, text = asr.transcription[0]
        assert start == pytest.approx(ZH_TIMESTAMP[0][1])
        assert end == pytest.approx(ZH_TIMESTAMP[-1][2])
        assert text == ZH_TEXT
        assert asr.score == pytest.approx(ZH_CONFIDENCE, abs=1e-4)

    def test__result_without_timestamps(self) -> None:
        result = {"uttid": "utt0", "text": ZH_TEXT, "confidence": 0.9, "dur_s": 3.5}
        asr = ASRResult.from_fireredasr2_result(result, index=SegmentIndex(audio=1))

        assert len(asr.transcription) == 1
        start, end, text = asr.transcription[0]
        assert start == pytest.approx(0.0)
        assert end == pytest.approx(3.5)
        assert text == ZH_TEXT

    def test__result_empty_text(self) -> None:
        result = {"uttid": "utt0", "text": "", "confidence": 0.5, "dur_s": 1.0}
        asr = ASRResult.from_fireredasr2_result(result, index=SegmentIndex(audio=0))
        assert asr.transcription == []

    def test__result_whitespace_only_text(self) -> None:
        result = {"uttid": "utt0", "text": "   ", "confidence": 0.5, "dur_s": 1.0}
        asr = ASRResult.from_fireredasr2_result(result, index=SegmentIndex(audio=0))
        assert asr.transcription == []


# Inference


class TestFireRedASR2InferenceRunner:
    def setup_method(self) -> None:
        mock_config = FireRedASR2InferenceRunnerConfig()

        self._inference_runner = MockFireRedASR2InferenceRunner(config=mock_config)

    def test__yields_asr_results(self, tmp_path: Path) -> None:
        """Inference runner should yield one ASRResult per segment in the batch"""
        preprocessor = FireRedASR2Preprocessor(
            batcher=ConstantSizeBatcherConfig(batch_size=4)
        )
        # Use audio shorter than max so each input produces exactly one segment
        audio = [
            torch.zeros(FIREREDASR2_INFERENCE_MAX_FRAMES // 2),
            torch.zeros(FIREREDASR2_INFERENCE_MAX_FRAMES // 2),
        ]

        batches = list(preprocessor.process(audio, output_dir=tmp_path))

        with self._inference_runner:
            results = list(self._inference_runner.process(batches))

        assert len(results) == 2
        assert all(isinstance(r, ASRResult) for r in results)
        assert [r.index.audio for r in results] == [0, 1]

    def test__skips_empty_batches(self) -> None:
        """Empty batches should not cause errors and should be silently skipped"""
        with self._inference_runner:
            results = list(self._inference_runner.process([[], []]))
        assert results == []

    @pytest.mark.no_ci
    def test__cache_to_dir_and_load_from_it(self) -> None:
        # Given
        # Let's use the local cache to avoid downloading for ages
        cache_dir = HF_HUB_CACHE
        # When
        cache_fireredasr2_models(cache_dir)
        runner = FireRedASR2InferenceRunner.from_config(
            FireRedASR2InferenceRunnerConfig()
        )
        with runner:
            # Then
            assert isinstance(runner._model, FireRedAsr2)

    @pytest.mark.no_ci
    def test__load_asr_without_cache(self) -> None:
        # When
        runner = FireRedASR2InferenceRunner.from_config(
            FireRedASR2InferenceRunnerConfig()
        )
        # When
        with runner:
            # Then
            assert isinstance(runner._model, FireRedAsr2)
