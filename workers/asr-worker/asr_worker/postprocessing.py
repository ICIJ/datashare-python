import logging
from asyncio import AbstractEventLoop
from collections.abc import Iterable
from functools import partial
from pathlib import Path
from typing import Protocol

from caul_core import ASRResult, Postprocessor
from caul_core import Error as CaulError
from datashare_python.types_ import SyncProgressRateHandler
from datashare_python.utils import write_artifact

from .objects import (
    ASRArgs,
    DocRoutes,
    Transcription,
    TranscriptionArtifact,
    TranscriptionManifestEntry,
)

logger = logging.getLogger()


class ArtifactFactory(Protocol):
    def __call__(self, artifact: bytes) -> TranscriptionArtifact: ...


def postprocess_act(
    inference_results: Iterable[ASRResult],
    audio_routes: DocRoutes,
    postprocessor: Postprocessor,
    args: ASRArgs,
    *,
    artifacts_root: Path,
    event_loop: AbstractEventLoop | None = None,
    progress: SyncProgressRateHandler | None = None,
) -> tuple[DocRoutes, list[CaulError]]:
    successes, errors = dict(), []
    for i, asr_result in enumerate(postprocessor.process(inference_results)):
        if isinstance(asr_result, CaulError):
            errors.append(asr_result)
        else:
            doc_id = asr_result.index.audio
            if not isinstance(doc_id, (str, int)):
                msg = "audios are expected to be indexed by doc ids"
                raise TypeError(msg)
            doc_root = audio_routes[doc_id]
            successes[doc_id] = doc_root
            manifest_entry = TranscriptionManifestEntry.complete(
                args, confidence=asr_result.score
            )
            artifact_factory = partial(
                TranscriptionArtifact,
                project=args.project,
                doc_id=doc_id,
                manifest_entry=manifest_entry,
            )
            t_path = write_transcription(asr_result, artifact_factory, artifacts_root)
            logger.debug("wrote transcription for %s", t_path)
        if progress is not None and event_loop is not None:
            progress(i, event_loop)
    return successes, errors


def write_transcription(
    asr_result: ASRResult, artifact_factory: ArtifactFactory, artifacts_root: Path
) -> Path:  # noqa: F821
    result = Transcription.from_asr_handler_result(asr_result)
    artifact_bytes = result.model_dump_json().encode()
    artifact = artifact_factory(artifact=artifact_bytes)
    rel_path = write_artifact(artifacts_root, artifact)
    return rel_path
