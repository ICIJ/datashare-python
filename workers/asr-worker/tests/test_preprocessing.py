import itertools
from collections.abc import Iterable
from pathlib import Path
from typing import Self

from asr_worker.config import ASRWorkerConfig
from asr_worker.preprocessing import preprocess_act
from caul_core import (
    ASRInput,
    FSProcessedSegment,
    Preprocessor,
    ProcessedAudioSegment,
    SegmentIndex,
    SegmentMetadata,
)
from caul_core import Error as CaulError
from caul_core.asr_task import SampleRate
from conftest import TEST_PROJECT
from datashare_python.objects import DatashareFile, DatashareLanguage, Document, Error
from icij_common.registrable import RegistrableConfig

PROCESSED_SEG_0 = FSProcessedSegment(
    path=Path("preprocessed_0.wav"),
    metadata=SegmentMetadata(index=SegmentIndex(audio=0), duration_s=0.0),
)
PROCESSED_SEG_1 = FSProcessedSegment(
    path=Path("preprocessed_1.wav"),  # noqa: F821
    metadata=SegmentMetadata(index=SegmentIndex(audio=1), duration_s=1.0),
)
PROCESSED_SEG_2 = FSProcessedSegment(
    path=Path("preprocessed_2.wav"),
    metadata=SegmentMetadata(index=SegmentIndex(audio=2), duration_s=2.0),
)
PROCESSED_SEG_ERROR_0 = CaulError(
    title="AudioDecodingError",
    detail="failed to decode audio",
    metadata=SegmentMetadata(index=SegmentIndex(audio=0), duration_s=0.0),
)


class MockPreprocessor(Preprocessor):
    def __init__(self, batch_size: int) -> None:
        self._batch_size = batch_size

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls(**kwargs)

    def process(
        self,
        inputs: ASRInput,
        sample_rates: SampleRate | None = None,  # noqa: ARG002
        output_dir: Path | None = None,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[tuple[ProcessedAudioSegment, ...] | Error]:
        outputs = itertools.cycle([PROCESSED_SEG_0, PROCESSED_SEG_1, PROCESSED_SEG_2])
        outputs = [next(outputs) for _ in inputs]
        yield from itertools.batched(outputs, self._batch_size)


class MockErroringPreprocessor(Preprocessor):
    def __init__(self, batch_size: int) -> None:
        self._batch_size = batch_size

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls(**kwargs)

    def process(
        self,
        inputs: ASRInput,
        sample_rates: SampleRate | None = None,  # noqa: ARG002
        output_dir: Path | None = None,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[tuple[ProcessedAudioSegment, ...] | Error]:
        outputs = itertools.cycle([PROCESSED_SEG_ERROR_0, PROCESSED_SEG_1])
        for _ in inputs:
            batch = next(outputs)
            if isinstance(batch, CaulError):
                yield batch
            else:
                yield (batch,)


def test_preprocess_act(test_worker_config: ASRWorkerConfig, tmpdir: Path) -> None:
    # Given
    output_dir = Path(tmpdir)
    n_audios = 3
    batch_size = n_audios - 1
    batch = [
        Document(
            id=f"doc-{i}",
            language=DatashareLanguage("ENGLISH"),
            root_document=f"root-{i}",
            extraction_level=1,
            path=Path(str(i)),
            index=TEST_PROJECT,
            metadata={"tika_metadata_resourcename": f"doc-{i}.wav"},
        )
        for i in range(n_audios)
    ]
    batch = [DatashareFile.from_parent(doc) for doc in batch]
    preprocessor = MockPreprocessor(batch_size=batch_size)

    # When
    batches, errors, audio_routes = preprocess_act(
        preprocessor, batch, test_worker_config, output_dir=output_dir
    )

    # Then
    assert len(batches) == 2
    expected_batches = [(PROCESSED_SEG_0, PROCESSED_SEG_1), (PROCESSED_SEG_2,)]
    assert batches == expected_batches
    assert not errors
    expected_audio_routes = dict(b.route for b in batch)
    assert audio_routes == expected_audio_routes


def test_preprocess_act_reports_error(
    test_worker_config: ASRWorkerConfig, tmpdir: Path
) -> None:
    # Given
    output_dir = Path(tmpdir)
    n_audios = 2
    batch_size = n_audios
    batch = [
        Document(
            id=f"doc-{i}",
            language=DatashareLanguage("ENGLISH"),
            path=Path(str(i)),
            root_document=f"root-{i}",
            extraction_level=1,
            index=TEST_PROJECT,
            metadata={"tika_metadata_resourcename": f"doc-{i}.wav"},
        )
        for i in range(n_audios)
    ]
    batch = [DatashareFile.from_parent(doc) for doc in batch]
    preprocessor = MockErroringPreprocessor(batch_size=batch_size)

    # When
    batches, errors, audio_routes = preprocess_act(
        preprocessor, batch, test_worker_config, output_dir=output_dir
    )

    # Then
    assert batches == [(PROCESSED_SEG_1,)]
    assert errors == [PROCESSED_SEG_ERROR_0]
    expected_audio_routes = dict(b.route for b in batch)
    assert audio_routes == expected_audio_routes
