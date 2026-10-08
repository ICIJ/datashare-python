from pathlib import Path

import pytest
import torch
from caul.tasks.postprocessing.asr_postprocessor import PostprocessorMixin
from caul.tasks.preprocessing.asr_preprocessor import ASRPreprocessorMixin
from caul_core import FIREREDASR2_INFERENCE_MAX_FRAMES, ASRResult, SegmentIndex


class TestASRPreprocessor:
    def setup_method(self) -> None:
        self._preprocessor = ASRPreprocessorMixin()

    def test__short_audio_single_segment(self) -> None:
        """Audio shorter than 60 seconds should produce exactly one segment"""
        audio = [torch.zeros(FIREREDASR2_INFERENCE_MAX_FRAMES * 10)]  # 10 s

        result = list(self._preprocessor.preprocess_inputs(audio))

        assert len(result) == 1
        assert result[0].metadata.index == SegmentIndex(audio=0, segment=0)

    def test__long_audio_gets_segmented(self) -> None:
        """Audio longer than 60 seconds must be split into multiple segments"""
        # 70 s of silence — segment_by_silence will fall back to fixed splits
        audio = [torch.zeros(FIREREDASR2_INFERENCE_MAX_FRAMES * 70)]

        result = list(self._preprocessor.preprocess_inputs(audio))

        assert len(result) > 1
        for i, seg in enumerate(result):
            assert seg.metadata.index == SegmentIndex(audio=0, segment=i)

    def test__multiple_inputs_ordering(self) -> None:
        """Index must match the original list index"""
        audio = [
            torch.zeros(FIREREDASR2_INFERENCE_MAX_FRAMES * 5),
            torch.zeros(FIREREDASR2_INFERENCE_MAX_FRAMES * 3),
        ]

        result = list(self._preprocessor.preprocess_inputs(audio))

        orderings = [r.metadata.index for r in result]
        assert orderings == [
            SegmentIndex(audio=0, segment=0),
            SegmentIndex(audio=1, segment=0),
        ]

    def test__write_wavs_to_fs(self, tmpdir: Path) -> None:
        """When output_dir is provided, wav files are written to disk"""
        output_dir = Path(tmpdir)
        audio = [torch.zeros(FIREREDASR2_INFERENCE_MAX_FRAMES * 2)]

        result = list(
            self._preprocessor.preprocess_inputs(audio, output_dir=output_dir)
        )

        assert len(result) == 1
        saved = output_dir / result[0].path
        assert saved.exists()


class TestASRPostprocessor:
    def setup_method(self) -> None:
        self._postprocessor = PostprocessorMixin()

    def test__merges_segments(self) -> None:
        """Segments belonging to the same index are merged in time order"""
        results = [
            ASRResult(
                index=SegmentIndex(audio=0, segment=0),
                transcription=[(0.0, 1.0, "你好")],
                score=0.9,
            ),
            ASRResult(
                index=SegmentIndex(audio=0, segment=1),
                transcription=[(1.0, 2.0, "世界")],
                score=0.8,
            ),
        ]

        merged = list(self._postprocessor.process(results))

        assert len(merged) == 1
        assert merged[0].index == SegmentIndex(audio=0, segment=0)
        assert merged[0].transcription == [(0.0, 1.0, "你好"), (1.0, 2.0, "世界")]

    def test__multiple_inputs(self) -> None:
        """Each unique index yields exactly one merged result"""
        results = [
            ASRResult(
                index=SegmentIndex(audio=0, segment=0),
                transcription=[(0.0, 1.0, "你好")],
                score=0.9,
            ),
            ASRResult(
                index=SegmentIndex(audio=1, segment=0),
                transcription=[(0.0, 2.0, "世界")],
                score=0.8,
            ),
        ]

        merged = list(self._postprocessor.process(results))

        assert len(merged) == 2
        expected_idx = [
            SegmentIndex(audio=0, segment=0),
            SegmentIndex(audio=1, segment=0),
        ]
        assert [r.index for r in merged] == expected_idx

    def test__drops_empty_transcription_segments(self) -> None:
        """Segments with empty transcriptions must be dropped before merging"""
        results = [
            ASRResult(
                index=SegmentIndex(audio=0, segment=0), transcription=[], score=1.0
            ),  # silent segment
            ASRResult(
                index=SegmentIndex(audio=0, segment=1),
                transcription=[(0.5, 1.5, "你好")],
                score=0.9,
            ),
        ]

        merged = list(self._postprocessor.process(results))

        assert len(merged) == 1
        assert merged[0].transcription == [(0.5, 1.5, "你好")]

    def test__raises_for_already_processed_audio(self) -> None:
        """Non-contiguous ordering (interleaved groups) should raise ValueError"""
        results = [
            ASRResult(
                index=SegmentIndex(audio=0, segment=0),
                transcription=[(0.0, 1.0, "a")],
                score=1.0,
            ),
            ASRResult(
                index=SegmentIndex(audio=1, segment=0),
                transcription=[(0.0, 1.0, "b")],
                score=1.0,
            ),
            ASRResult(
                index=SegmentIndex(audio=0, segment=1),
                transcription=[(1.0, 2.0, "a again")],
                score=1.0,
            ),
        ]

        expected = "expected contiguous segments, already processed segments from audio"
        with pytest.raises(ValueError, match=expected):
            list(self._postprocessor.process(results))

    def test__raises_for_unordered_segments_audio(self) -> None:
        """Non-contiguous ordering (interleaved groups) should raise ValueError"""
        results = [
            ASRResult(
                index=SegmentIndex(audio=0, segment=1),
                transcription=[(0.0, 1.0, "b")],
                score=1.0,
            ),
            ASRResult(
                index=SegmentIndex(audio=0, segment=0),
                transcription=[(0.0, 1.0, "a")],
                score=1.0,
            ),
        ]

        with pytest.raises(
            ValueError, match="received audio segment results for audio 0 out of order"
        ):
            list(self._postprocessor.process(results))

    def test__inputs_all_silent(self) -> None:
        """An input whose segments are silent should yield an empty transcription"""
        results = [
            ASRResult(
                index=SegmentIndex(audio=0, segment=0), transcription=[], score=1.0
            ),
            ASRResult(
                index=SegmentIndex(audio=0, segment=1), transcription=[], score=1.0
            ),
        ]

        merged = list(self._postprocessor.process(results))

        assert len(merged) == 1
        assert merged[0].transcription == []
