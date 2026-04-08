import json
import math
from collections.abc import Iterable
from pathlib import Path
from typing import Self

from asr_worker.activities import preprocess, write_transcription
from asr_worker.models import Timestamp, Transcript, Transcription
from asr_worker.utils import read_jsonl
from caul.objects import ASRResult, InputMetadata, PreprocessedInput
from caul.tasks import Preprocessor
from datashare_python.conftest import TEST_PROJECT
from icij_common.registrable import RegistrableConfig

PREPROCESSED_INPUT_0 = PreprocessedInput(metadata=InputMetadata(duration_s=0.0))
PREPROCESSED_INPUT_1 = PreprocessedInput(metadata=InputMetadata(duration_s=1.0))
PREPROCESSED_INPUT_2 = PreprocessedInput(metadata=InputMetadata(duration_s=2.0))


class MockProcessor(Preprocessor):
    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls(**kwargs)

    def __init__(self, batches: list[list[PreprocessedInput]]) -> None:
        self._batches = batches

    def process(
        self,
        audios: Iterable[Path],  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[list[PreprocessedInput]]:
        yield from self._batches


def test_preprocess(tmpdir: Path) -> None:
    # Given
    output_dir = Path(tmpdir)
    batches = [[PREPROCESSED_INPUT_0, PREPROCESSED_INPUT_1], [PREPROCESSED_INPUT_2]]
    preprocessor = MockProcessor(batches)
    audios = []
    # When
    batch_files = list(preprocess(preprocessor, audios, output_dir=output_dir))
    # Then
    assert len(batch_files) == 2
    written_batches = [
        [PreprocessedInput.model_validate(d) for d in read_jsonl(f)]
        for f in batch_files
    ]
    assert written_batches == batches


def test_write_transcription(tmpdir: Path) -> None:
    # Given
    asr_result = ASRResult(transcription=[(0.0, 1.0, "text")], score=math.log(0.5))
    transcribed_filename = "0011someid"
    artifacts_root = Path(tmpdir)
    project = TEST_PROJECT
    # When
    write_transcription(
        asr_result, transcribed_filename, artifacts_root=artifacts_root, project=project
    )
    # Then
    expected_artifact_dir = artifacts_root / project / "00" / "11" / "0011someid"
    assert expected_artifact_dir.exists()
    metadata_path = expected_artifact_dir / "metadata.json"
    assert metadata_path.exists()
    metadata = json.loads(metadata_path.read_text())
    assert metadata["transcription"] == "transcription.json"
    transcription_path = expected_artifact_dir / metadata["transcription"]
    assert transcription_path.exists()
    transcription = Transcription.model_validate_json(transcription_path.read_text())
    expected_transcription = Transcription(
        transcripts=[
            Transcript(text="text", timestamp=Timestamp(start_s=0.0, end_s=1.0))
        ],
        confidence=0.5,
    )
    assert transcription == expected_transcription
