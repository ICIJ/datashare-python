import logging
import math
from collections import defaultdict

from datashare_python.types_ import ProgressRateHandler
from elasticsearch._async.helpers import async_bulk
from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, ID_, ESClient

from .objects_ import NER, BatchDocument, Category, NamedEntity, NERTagger, NlpTag


async def extract_and_write_doc_ner_tags(
    docs: list[BatchDocument],
    ner_tagger: NERTagger,
    categories: set[Category] | None,
    es_client: ESClient,
    *,
    batch_size: int,
    max_length: int,
    ne_buffer_size: int,
    progress: ProgressRateHandler | None,
    logger: logging.Logger,
) -> int | None:
    extractor = NER.SPACY
    batch = []
    n_docs = len(docs)
    if not n_docs:
        return None
    project = docs[0].project
    tags_buffer = _TagsBuffer(max_length)
    ne_buffer = []
    # Let's not overload the broker with events and only log progress halfway
    progress_rate = len(docs) / 2
    for doc_i, doc in enumerate(docs):
        logger.debug("processing doc: (%s/%s)", doc_i, n_docs)
        es_doc = await es_client.get_source(
            index=doc.project,
            id=doc.id,
            routing=doc.root_document,
            _source_includes=[DOC_LANGUAGE, DOC_CONTENT],
        )
        logger.debug("got docs...")
        tags_buffer.add_doc(doc_i, es_doc)
        doc_content = es_doc[DOC_CONTENT]
        doc_length = len(doc_content)
        for text_i in range(0, doc_length, max_length):
            chunk = doc_content[text_i : text_i + max_length]
            batch.append(chunk)
            tags_buffer.set_batch_doc_ix(doc_i)
            if len(batch) >= batch_size:
                batch_tags = [
                    text_tags async for text_tags in ner_tagger(batch, categories)
                ]
                ready_ents = await _update_and_consume_buffer(
                    tags_buffer, batch_tags, docs, extractor=extractor
                )
                ne_buffer += ready_ents
                if len(ne_buffer) >= ne_buffer_size:
                    await _bulk_add_ne(es_client, ne_buffer, project=project)
                    ne_buffer.clear()
                batch.clear()
        if progress is not None and not doc_i % progress_rate:
            await progress(doc_i / n_docs)
        logger.debug("processed doc: (%s/%s)", doc_i, n_docs)
    if batch:
        batch_tags = [text_tags async for text_tags in ner_tagger(batch, categories)]
        ready_ents = await _update_and_consume_buffer(
            tags_buffer, batch_tags, docs, extractor
        )
        if len(tags_buffer):
            raise ValueError(
                "inconsistent state: tags buffer was not empty processing all docs"
            )
        ne_buffer += ready_ents
    if ne_buffer:
        await _bulk_add_ne(es_client, ne_buffer, project=project)
    return n_docs


class _TagsBuffer:
    def __init__(self, max_length: int):
        self._max_length = max_length
        self._batch_ix_to_doc_ix: dict[int, int] = dict()
        self._n_doc_chunks: dict[int, int] = dict()
        self._doc_tags: dict[int, list[list[NlpTag]]] = defaultdict(list)

    def add_doc(self, i: int, doc: dict) -> None:
        self._n_doc_chunks[i] = math.ceil(len(doc[DOC_CONTENT]) / self._max_length)

    def add_batch_tags(self, batch_tags: list[list[NlpTag]]) -> None:
        for tag_i, tags in enumerate(batch_tags):
            chunks_tags = self._doc_tags[self._batch_ix_to_doc_ix[tag_i]]
            offset = self._max_length * len(chunks_tags)
            chunks_tags.append([t.with_offset(offset) for t in tags])
        for doc_i, tags in self._doc_tags.items():
            if len(tags) != self._n_doc_chunks[doc_i]:
                continue
        self._batch_ix_to_doc_ix.clear()

    def set_batch_doc_ix(self, doc_ix: int) -> None:
        new_ix = len(self._batch_ix_to_doc_ix)
        self._batch_ix_to_doc_ix[new_ix] = doc_ix

    def get_ready(self) -> dict[int, list[NlpTag]]:
        ready = {
            doc_i: sum(tags, start=[])
            for doc_i, tags in self._doc_tags.items()
            if len(tags) == self._n_doc_chunks[doc_i]
        }
        for doc_i in ready:
            self._n_doc_chunks.pop(doc_i)
            self._doc_tags.pop(doc_i)
        return ready

    def __len__(self):
        return len(self._n_doc_chunks)


async def _update_and_consume_buffer(
    tags_buffer: _TagsBuffer,
    batch_tags: list[list[NlpTag]],
    docs: list[BatchDocument],
    extractor: NER,
) -> list[list[NamedEntity]]:
    tags_buffer.add_batch_tags(batch_tags)
    ready_ents = sum(
        (
            NamedEntity.from_tags(tags, docs[ready_i], extractor=extractor)
            for ready_i, tags in tags_buffer.get_ready().items()
        ),
        [],
    )
    return ready_ents


async def _bulk_add_ne(
    es_client: ESClient, named_entities: list[NamedEntity], project: str
) -> None:
    named_entities = [ne.model_dump() for ne in named_entities]
    actions = (
        {
            "_op_type": "update",
            "_index": project,
            ID_: ne["id"],
            "doc": ne,
            "doc_as_upsert": True,
        }
        for ne in named_entities
    )
    # TODO: make the refresh parameter configurable as it only needs to be set for
    #  testing
    await async_bulk(es_client, actions, raise_on_error=True, refresh="wait_for")
