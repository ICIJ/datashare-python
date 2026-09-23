from collections.abc import Iterable
from pathlib import Path
from typing import Self

from asr_worker.inference import infer_act
from caul_core import (
    ASRResult,
    FSProcessedSegment,
    InferenceRunner,
    SegmentIndex,
    SegmentMetadata,
)
from caul_core import Error as CaulError
from icij_common.registrable import RegistrableConfig

PROCESSED_SEG_0 = FSProcessedSegment(
    path=Path("preprocessed_0.wav"),
    metadata=SegmentMetadata(index=SegmentIndex(audio=0), duration_s=0.0),
)
PROCESSED_SEG_1 = FSProcessedSegment(
    path=Path("preprocessed_1.wav"),
    metadata=SegmentMetadata(index=SegmentIndex(audio=1), duration_s=1.0),
)
PROCESSED_SEG_2 = FSProcessedSegment(
    path=Path("preprocessed_2.wav"),
    metadata=SegmentMetadata(index=SegmentIndex(audio=2), duration_s=2.0),
)


INFERENCE_RESULTS = [
    ASRResult(
        index=SegmentIndex(audio=0),
        transcription=[(0.0, 0.0, "preprocessed_0")],
        score=1.0,
    ),
    ASRResult(
        index=SegmentIndex(audio=1),
        transcription=[(0.0, 1.0, "preprocessed_1")],
        score=1.0,
    ),
    ASRResult(
        index=SegmentIndex(audio=2),
        transcription=[(0.0, 2.0, "preprocessed_2")],
        score=1.0,
    ),
]


class MockInferenceRunner(InferenceRunner):
    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls()

    def process(
        self,
        inputs: Iterable[list[FSProcessedSegment]],
        *args,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ASRResult | CaulError]:
        i = 0
        for batch in inputs:
            for preprocessed in batch:
                transcription = preprocessed.path.name.replace(".wav", "")
                transcription = [(0.0, float(i), transcription)]
                index = SegmentIndex(audio=i)
                yield ASRResult(index=index, transcription=transcription, score=1.0)
                i += 1


class MockFailingInferenceRunner(InferenceRunner):
    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls()

    def process(
        self,
        inputs: Iterable[list[FSProcessedSegment]],
        *args,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ASRResult | CaulError]:
        for batch in inputs:
            for seg in batch:
                yield CaulError(
                    title="AudioInferenceError",
                    detail="failed to infer on segment audio",
                    metadata=seg.metadata,
                )


async def test_infer_act(tmpdir: Path) -> None:
    # Given
    inference_runner = MockInferenceRunner()
    workdir = Path(tmpdir) / "workdir"
    workdir.mkdir()
    output_dir = Path(tmpdir)
    batches = [(PROCESSED_SEG_0, PROCESSED_SEG_1), (PROCESSED_SEG_2,)]
    # When
    successes, errors = await infer_act(
        inference_runner, batches=batches, output_dir=output_dir
    )
    # Then
    res = [
        ASRResult.model_validate_json((output_dir / p).read_text()) for p in successes
    ]
    assert not errors
    assert res == INFERENCE_RESULTS


async def test_infer_act_should_report_errors(tmpdir: Path) -> None:
    # Given
    inference_runner = MockFailingInferenceRunner()
    workdir = Path(tmpdir) / "workdir"
    workdir.mkdir()
    output_dir = Path(tmpdir)
    batches = [(PROCESSED_SEG_0,)]
    # When
    _, errors = await infer_act(
        inference_runner, batches=batches, output_dir=output_dir
    )
    # Then
    expected_error = CaulError(
        title="AudioInferenceError",
        detail="failed to infer on segment audio",
        metadata=PROCESSED_SEG_0.metadata,
    )
    assert errors == [expected_error]
