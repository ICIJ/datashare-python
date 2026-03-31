import json
import math
from pathlib import Path

from asr_worker.activities import write_transcription
from asr_worker.models import Timestamp, Transcript, Transcription
from caul.model_handlers.objects import ASRModelHandlerResult
from datashare_python.conftest import TEST_PROJECT


def test_write_transcription(tmpdir: Path) -> None:
    # Given
    asr_result = ASRModelHandlerResult(
        transcription=[(0.0, 1.0, "text")], score=math.log(0.5)
    )
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
