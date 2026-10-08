import inspect
import math
import sys
from pathlib import Path
from typing import Never

import pytest
import torch
import torchcodec.decoders
from caul.tasks.preprocessing.asr_preprocessor import ASRPreprocessorMixin
from caul.utils import load_audio
from caul_core import DEFAULT_SAMPLE_RATE, Error
from torchcodec.decoders import AudioDecoder
from torchcodec.encoders import AudioEncoder

from tests.unit.constant import TEST_WAV_PATH


@pytest.fixture
def long_wav(tmp_path: Path) -> Path:
    """3-second silent wav at 16 kHz, long enough for several 1-second chunks"""
    audio = torch.zeros(1, DEFAULT_SAMPLE_RATE * 3)
    path = tmp_path / "long.wav"
    AudioEncoder(audio, sample_rate=DEFAULT_SAMPLE_RATE).to_file(path)
    return path


def _chunking_preprocessor(
    max_frames: int = DEFAULT_SAMPLE_RATE,
) -> ASRPreprocessorMixin:
    """Preprocessor with threshold=1 so every file triggers the chunked path"""
    return ASRPreprocessorMixin(large_file_threshold_bytes=1, max_frames=max_frames)


def test_load_stereo_24bit_audio() -> None:
    tensor = load_audio(TEST_WAV_PATH)
    assert len(tensor.shape) == 1


class TestASRPreprocessorChunking:
    def test__below_threshold_loads_eagerly(self, long_wav: Path) -> None:
        """Files under the byte threshold produce exactly one chunk"""
        preprocessor = ASRPreprocessorMixin(large_file_threshold_bytes=sys.maxsize)
        chunks = list(
            preprocessor._load_file_as_chunks(long_wav, DEFAULT_SAMPLE_RATE),
        )
        assert len(chunks) == 1

    def test__above_threshold_produces_multiple_chunks(self, long_wav: Path) -> None:
        """Files over the byte threshold are split into several time-window chunks"""
        chunks = list(
            _chunking_preprocessor()._load_file_as_chunks(long_wav, DEFAULT_SAMPLE_RATE)
        )
        assert len(chunks) > 1

    def test__chunk_count_matches_expected(self, long_wav: Path) -> None:
        """Number of chunks equals ceil(file_duration / chunk_duration)"""
        preprocessor = _chunking_preprocessor()
        duration_s = AudioDecoder(long_wav).metadata.duration_seconds
        chunk_duration_s = DEFAULT_SAMPLE_RATE / DEFAULT_SAMPLE_RATE
        expected = math.ceil(duration_s / chunk_duration_s)

        chunks = list(preprocessor._load_file_as_chunks(long_wav, DEFAULT_SAMPLE_RATE))
        assert len(chunks) == expected

    def test__each_chunk_at_most_max_frames(self, long_wav: Path) -> None:
        """No chunk contains more than max_frames samples"""
        chunks = list(
            _chunking_preprocessor()._load_file_as_chunks(long_wav, DEFAULT_SAMPLE_RATE)
        )
        for chunk in chunks:
            # +1 tolerance for floating-point rounding in the decoder's resampler
            assert chunk.shape[-1] <= DEFAULT_SAMPLE_RATE + 1

    def test__chunks_are_1d_tensors(self, long_wav: Path) -> None:
        """Chunks are 1D (mono, squeezed) tensors at the target sample rate"""
        chunks = list(
            _chunking_preprocessor()._load_file_as_chunks(long_wav, DEFAULT_SAMPLE_RATE)
        )
        for chunk in chunks:
            assert len(chunk.shape) == 1

    def test__load_file_as_chunks_is_lazy(self, long_wav: Path) -> None:
        """_load_file_as_chunks must return a generator, not a materialized list"""
        result = _chunking_preprocessor()._load_file_as_chunks(
            long_wav, DEFAULT_SAMPLE_RATE
        )
        assert inspect.isgenerator(result)

    def test__chunking_triggered_by_byte_threshold(self, long_wav: Path) -> None:
        """Threshold comparison uses byte size derived from file metadata"""
        meta = AudioDecoder(long_wav).metadata
        estimated_bytes = (
            int(meta.duration_seconds * meta.sample_rate) * meta.num_channels * 4
        )

        eager = ASRPreprocessorMixin(
            large_file_threshold_bytes=estimated_bytes + 1,
            max_frames=DEFAULT_SAMPLE_RATE,
        )
        chunked = ASRPreprocessorMixin(
            large_file_threshold_bytes=estimated_bytes - 1,
            max_frames=DEFAULT_SAMPLE_RATE,
        )

        assert len(list(eager._load_file_as_chunks(long_wav, DEFAULT_SAMPLE_RATE))) == 1
        assert (
            len(list(chunked._load_file_as_chunks(long_wav, DEFAULT_SAMPLE_RATE))) > 1
        )

    def test__seg_i_contiguous_across_chunks(
        self, long_wav: Path, tmp_path: Path
    ) -> None:
        """Segment output file indices are sequential even across chunk boundaries"""
        preprocessor = _chunking_preprocessor()
        output_dir = tmp_path / "segments"
        output_dir.mkdir()
        results = list(
            preprocessor.preprocess_inputs([long_wav], output_dir=output_dir)
        )
        indices = sorted(int(f.stem.rsplit("-", 1)[-1]) for f in output_dir.iterdir())
        assert indices == list(range(len(results)))

    def test__all_segments_share_index(self, long_wav: Path) -> None:
        """All segments produced from one file have the same index"""
        results = list(_chunking_preprocessor().preprocess_inputs([long_wav]))
        assert all(r.metadata.index.audio == 0 for r in results)

    def test__two_large_files_have_distinct_orderings(self, long_wav: Path) -> None:
        """Two chunked files produce segments with index 0 and 1"""
        results = list(_chunking_preprocessor().preprocess_inputs([long_wav, long_wav]))
        assert {r.metadata.index.audio for r in results} == {0, 1}

    def test__should_index_by_provided_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ValueError during decoding (as, for instance, when there is no audio stream
        is kept as an error record to preserve input ordering
        """

        def _raise(*_args, **_kwargs) -> Never:
            raise ValueError(
                "The best audio stream is unknown and there is no specified stream."
            )

        monkeypatch.setattr(torchcodec.decoders, "AudioDecoder", _raise)

        results = list(
            ASRPreprocessorMixin().preprocess_inputs(["no_audio_stream.mp4"])
        )

        assert len(results) == 1
        assert isinstance(results[0], Error)
        error = results[0]
        assert error.title == "UnreadableAudio"

    def test__decode_failure_yields_error_record_instead_of_raising(
        self, long_wav: Path
    ) -> None:
        inputs = [("some_id", long_wav)]
        results = list(_chunking_preprocessor().preprocess_inputs(inputs))
        assert all(r.metadata.index.audio == "some_id" for r in results)
