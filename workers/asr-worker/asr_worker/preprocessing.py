import logging
from pathlib import Path

from caul_core import Error as CaulError
from caul_core import FSProcessedSegment, Preprocessor
from datashare_python.objects import DatashareFile

from .config import ASRWorkerConfig
from .objects import DocRoutes

logger = logging.getLogger(__name__)


def preprocess_act(
    preprocessor: Preprocessor,
    audios: list[DatashareFile],
    worker_config: ASRWorkerConfig,
    *,
    output_dir: Path,
) -> tuple[list[tuple[FSProcessedSegment]], list[CaulError], DocRoutes]:
    logger.debug("locating files...")
    # Read doc ids first
    processed_audios = ((a.doc_id, a.locate(worker_config.roots)) for a in audios)
    # TODO: implement a caching strategy here, we could avoid processing files
    #  which have already been preprocessed
    logger.debug("starting preprocessing...")
    batches, errors = [], []
    for batch in preprocessor.process(processed_audios, output_dir=output_dir):
        if isinstance(batch, CaulError):
            errors.append(batch)
        else:
            batches.append(batch)
    audio_routes = dict(a.route for a in audios)
    return batches, errors, audio_routes
