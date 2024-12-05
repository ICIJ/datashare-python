from typing import AsyncGenerator, Generator, Iterable

import torch
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    BOOL,
    COUNT,
    DOC_CONTENT,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    ESClient,
    HITS,
    ID_,
)
from icij_worker.typing_ import PercentProgress
from icij_worker.utils.progress import to_raw_progress
from transformers import Pipeline, pipeline

from ml_worker.objects import ClassificationConfig, Document
from ml_worker.utils import async_batches


async def classify_docs(
    es_client: ESClient,
    project: str,
    progress: PercentProgress | None = None,
    config: ClassificationConfig = ClassificationConfig(),
) -> int:
    model = config.model
    # Retrieve the number of docs which haven't been classified yet
    # (to publish progress)
    n_docs = await _count_unclassified(es_client, project, model=model)
    if not n_docs:
        if progress is not None:
            await progress(1.0)
        return n_docs
    # Convert the progress to a "raw" progress to update the progress incrementally
    # rather than setting the progress rate/percentage
    progress = to_raw_progress(progress, max_progress=n_docs)
    # Torch/macOS silicon stuff
    device = None
    if torch.backends.mps.is_available():
        device = torch.device("mps")
    # Load the classification pipeline
    pipe = pipeline(config.task, model=model, device=device)
    model = pipe.model.name_or_path
    seen = 0
    # Retrieve unprocessed docs.
    unclassified = _get_unclassified(es_client, project=project, model=model)
    # We batch the data ourselves, ideally, we should use an async version of:
    # https://huggingface.co/docs/datasets/v3.1.0/en/package_reference/main_classes#datasets.Dataset.from_generator
    async for batch in async_batches(unclassified, batch_size=config.batch_size):
        contents = [d.content for d in batch]
        labels = _classify(pipe, contents)
        # We add the classification results by updating the documents with new tags,
        # this could also be done using: https://github.com/ICIJ/datashare-tarentula
        await _add_classification_tags(
            es_client, zip(batch, labels), project, model=model
        )
        seen += len(batch)
        if progress is not None:
            await progress(seen)
    # Return the number of classified documents
    return n_docs


def _classify(pipe: Pipeline, texts: list[str]) -> Generator[str, None, None]:
    for res in pipe(texts):
        yield res["label"]


_SCRIPT_SOURCES = """
if( !ctx._source.containsKey("tags") ) {
    ctx._source.tags = [];
}
if( !ctx._source.tags.contains(params.tag) ) {
    ctx._source.tags.add(params.tag);
}
"""


async def _add_classification_tags(
    es_client: ESClient,
    tags: Iterable[tuple[Document, str]],
    project: str,
    *,
    model: str,
):
    actions = (
        {
            "_op_type": "update",
            "_index": project,
            "_routing": doc.root_document,
            ID_: doc.id,
            "script": {
                "source": _SCRIPT_SOURCES,
                "lang": "painless",
                "params": {"tag": f"classified:{model}:{label}"},
            },
        }
        for doc, label in tags
    )
    await async_bulk(es_client, actions, raise_on_error=True, refresh="wait_for")


def _unclassified_query(model: str):
    query = {
        "query": {
            BOOL: {"must_not": {"prefix": {"tags": {"value": f"classified:{model}:"}}}}
        }
    }
    return query


_DOC_SOURCES = [DOC_LANGUAGE, DOC_CONTENT, DOC_ROOT_ID, "project"]


async def _get_unclassified(
    es_client: ESClient, project: str, *, model: str
) -> AsyncGenerator[Document, None]:
    async for res in es_client.poll_search_pages(
        index=project,
        body=_unclassified_query(model),
        _source_includes=_DOC_SOURCES,
        sort="_doc:asc",
    ):
        for res in res[HITS][HITS]:
            yield Document.from_es(res)


async def _count_unclassified(es_client: ESClient, project: str, *, model: str) -> int:
    res = await es_client.count(index=project, body=_unclassified_query(model))
    return res[COUNT]
