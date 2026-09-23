import json
from collections.abc import Iterable
from pathlib import Path
from typing import Self

from asr_worker.objects import ASRArgs, Transcription, TranscriptionManifestEntry
from asr_worker.postprocessing import postprocess_act
from caul_core import ASRResult, AudioMetadata, Postprocessor, SegmentIndex
from caul_core import Error as CaulError
from conftest import TEST_PROJECT
from icij_common.registrable import RegistrableConfig

from .conftest import DS_ENGLISH

INFERENCE_RESULTS = [
    ASRResult(
        index=SegmentIndex(audio="0000-doc-0"),
        transcription=[(0.0, 0.0, "preprocessed_0")],
        score=1.0,
    ),
    ASRResult(
        index=SegmentIndex(audio="1111-doc-1"),
        transcription=[(0.0, 1.0, "preprocessed_1")],
        score=1.0,
    ),
    ASRResult(
        index=SegmentIndex(audio="2222-doc-2"),
        transcription=[(0.0, 2.0, "preprocessed_2")],
        score=1.0,
    ),
]


class MockPostprocessor(Postprocessor):
    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls()

    def process(
        self,
        inputs: Iterable[ASRResult],
        *args,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ASRResult | CaulError]:
        yield from inputs


class MockFailingPostprocessor(Postprocessor):
    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls()

    def process(
        self,
        inputs: Iterable[ASRResult],
        *args,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ASRResult | CaulError]:
        for res in inputs:
            yield CaulError(
                title="PostprocessingError",
                detail="failed to build transcription from segments",
                metadata=AudioMetadata(index=res.index.audio),
            )


def test_postprocess_act(tmpdir: Path) -> None:
    # Given
    args = ASRArgs(project=TEST_PROJECT, docs=[], batch_size=2, language=DS_ENGLISH)
    postprocessor = MockPostprocessor()
    project = TEST_PROJECT
    artifacts_root = Path(tmpdir)
    doc_routes = {f"{str(i) * 4}-doc-{i}": f"root-{i}" for i in range(3)}
    # When
    routes, errors = postprocess_act(
        INFERENCE_RESULTS,
        doc_routes,
        postprocessor,
        args,
        artifacts_root=artifacts_root,
    )
    # Then
    assert not errors
    assert routes == doc_routes
    expected_artifact_dirs = [
        artifacts_root / project / "00" / "00" / "0000-doc-0",
        artifacts_root / project / "11" / "11" / "1111-doc-1",
        artifacts_root / project / "22" / "22" / "2222-doc-2",
    ]
    for res, d in zip(INFERENCE_RESULTS, expected_artifact_dirs, strict=True):
        assert d.exists()
        manifest_path = d / "manifest.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        assert "transcription" in manifest
        manifest_entry = TranscriptionManifestEntry.model_validate(
            manifest["transcription"]
        )
        assert manifest_entry.confidence == 1
        assert manifest_entry.input
        transcription_path = d / "transcription.json"
        assert transcription_path.exists()
        transcription = Transcription.model_validate_json(
            transcription_path.read_text()
        )
        expected_transcription = Transcription.from_asr_handler_result(res)
        assert transcription == expected_transcription


def test_postprocess_act_reports_errors(tmpdir: Path) -> None:
    # Given
    args = ASRArgs(project=TEST_PROJECT, docs=[], batch_size=2, language=DS_ENGLISH)
    postprocessor = MockFailingPostprocessor()
    artifacts_root = Path(tmpdir)
    doc_routes = [(f"{str(i) * 4}-doc-{i}", f"root-{i}") for i in range(1)]
    seg_res = ASRResult(
        index=SegmentIndex(audio=0),
        transcription=[(0.0, 0.0, "preprocessed_0")],
        score=1.0,
    )
    res = [seg_res]
    # When
    successes, errors = postprocess_act(
        res, doc_routes, postprocessor, args, artifacts_root=artifacts_root
    )
    # Then
    assert not successes
    expected_error = CaulError(
        title="PostprocessingError",
        detail="failed to build transcription from segments",
        metadata=AudioMetadata(index=seg_res.index.audio),
    )
    assert errors == [expected_error]
