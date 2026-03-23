import functools
import multiprocessing

import pycountry
from datashare_python.dependencies import lifespan_es_client
from datashare_python.types_ import ProgressRateHandler
from datashare_python.utils import ActivityWithProgress, activity_defn, to_raw_progress
from spacy import Language
from temporalio import activity

from .core import extract_and_write_doc_ner_tags
from .dependencies import lifespan_spacy_pipeline_cache
from .objects_ import (
    BatchDocument,
    Category,
    SpacyConfig,
)
from .spacy_.ner import spacy_ner


class NERActivity(ActivityWithProgress):
    @activity_defn(name="spacy-batch-ner")
    async def spacy_ner(
        self,
        docs: list[BatchDocument],
        *,
        categories: list[Category] | None = None,
        config: SpacyConfig | None = None,
        max_length: int = 1024,
        max_processes: int = -1,
        ne_buffer_size: int = 1000,
        batch_size: int = 1024,
        pipeline_batch_size: int = 1024,
        progress: ProgressRateHandler | None = None,
    ) -> int:
        if config is None:
            config = SpacyConfig()
        logger = activity.logger
        if not docs:
            return len(docs)
        if progress is not None:
            progress = to_raw_progress(progress, max_progress=len(docs))
        categories = set(categories)
        spacy_provider = lifespan_spacy_pipeline_cache()
        es_client = lifespan_es_client()
        docs_language = docs[0].language
        language = pycountry.languages.get(name=docs_language)
        if language is None or not hasattr(language, "alpha_2"):
            raise ValueError(f'Unknown language "{docs_language}"')
        language = language.alpha_2
        ner = spacy_provider.get_ner(language, model_size=config.model_size)
        logger.info("ner loaded")
        sent_split = spacy_provider.get_sent_split(
            language, model_size=config.model_size
        )
        n_process = _resolve_n_process(ner, max_processes=max_processes)
        logger.info("start processing")

        ner_tagger = functools.partial(
            spacy_ner,
            ner=ner,
            sent_split=sent_split,
            n_process=n_process,
            progress=progress,
            batch_size=pipeline_batch_size,
        )

        # TODO: ideally this part shouldn't be spacy specific, the process_docs
        #  function should be pipeline agnostic
        return await extract_and_write_doc_ner_tags(
            docs,
            ner_tagger,
            categories,
            es_client,
            batch_size=batch_size,
            ne_buffer_size=ne_buffer_size,
            max_length=max_length,
            progress=progress,
            logger=logger,
        )


def _resolve_n_process(ner: Language, max_processes: int) -> int:
    if "transformer" in ner.pipe_names:
        return 1
    if max_processes == -1:
        max_processes = multiprocessing.cpu_count()
    return max_processes
