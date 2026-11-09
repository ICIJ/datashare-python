import asyncio
import gc
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import AsyncIterable, Collection, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from functools import partial
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from datashare_python.objects import BaseModel, Document
from elastic_transport._models import DefaultType
from elastic_transport.client_utils import DEFAULT
from elasticsearch.dsl import Query
from elasticsearch.dsl.query import (
    Bool,
    Ids,
    Knn,
    Match,
    MatchAll,
    MoreLikeThis,
    ScriptScore,
    Term,
)
from elasticsearch.dsl.types import LikeDocument, MatchQuery, Script
from icij_common.es import (
    COUNT,
    DOC_CONTENT,
    DOC_CONTENT_TYPE,
    DOC_EXTRACTION_LEVEL,
    DOC_LANGUAGE,
    DOC_PATH,
    DOC_ROOT_ID,
    ES_DOCUMENT_TYPE,
    HITS,
    ID_,
    QUERY,
    ROUTING_,
    SCORE_,
    SEARCH_AFTER,
    SORT,
    SOURCE,
    TOTAL,
    VALUE,
    ESClient,
)
from pydantic import Field

from .constants import (
    CHUNK_DOC_ID,
    CHUNK_EMBEDDING,
    CHUNK_ID,
    CHUNK_TEXT,
    DEFAULT_EMBEDDING_DIM,
)
from .embeddings import Embedder, chunk_text
from .objects import Vector

if TYPE_CHECKING:
    from docling_core.transforms.chunker import BaseChunker

logger = logging.getLogger(__name__)

Routing = tuple[str, str]


class SimilarityType(StrEnum):
    TERM_FREQUENCY = "term_frequency"
    EMBEDDING = "embedding"
    HYBRID = "hybrid"


@dataclass(frozen=True)
class QueryComponent:
    should: list[Query] = field(default_factory=list)
    filter: list[Query] = field(default_factory=list)
    must_not: list[Query] = field(default_factory=list)
    script_score: list[MoreLikeThis] = field(default_factory=list)

    _script_source = "Math.max(0, params.c - params.lambda * _score)"

    def __add__(self, other: Any) -> Self:
        if not isinstance(other, QueryComponent):
            msg = f"expected {QueryComponent.__name__}, found {other}"
            raise TypeError(msg)
        should = self.should + other.should
        filter_ = self.filter + other.filter
        must_not = self.must_not + other.must_not
        script_score = self.script_score + other.script_score
        return QueryComponent(
            should=should, filter=filter_, must_not=must_not, script_score=script_score
        )

    @property
    def unlike(self) -> Query | None:
        if not self.script_score:
            return None
        should = [MatchAll(boost=0.0)] + self.script_score
        lbd = _sum_boosts(self.script_score)
        lbd = lbd / (lbd + _sum_boosts(self.should))
        script = Script(source=self._script_source, params={"c": 1000, "lambda": lbd})
        script_score = ScriptScore(query=Bool(should=should), script=script)
        return script_score


class _ToES(BaseModel, ABC):
    @abstractmethod
    def to_es(self, **kwargs) -> QueryComponent: ...


class LikeDoc(_ToES):
    doc_id: str
    similarity: SimilarityType
    term_frequency_weight: float = 1.0
    embedding_weight: float = 1.0

    def to_es(
        self,
        top_k: int,
        vectors: Sequence[Vector] | None = None,
        *,
        doc_id_field: str,
        routings: Collection[Routing] | None = None,
        text_fields: list[str] | None = None,
        embedding_field: str | None = None,
    ) -> QueryComponent:
        # TODO: put me back
        # self._validate_args(
        #     self.similarity,
        #     vectors,
        #     text_fields=text_fields,
        #     embedding_field=embedding_field,
        # )
        match self.similarity:
            case SimilarityType.TERM_FREQUENCY:
                queries = [self._mtl(text_fields, routings)]
            case SimilarityType.EMBEDDING:
                queries = self._knns(vectors, top_k, embedding_field)
            case SimilarityType.HYBRID:
                queries = self._knns(vectors, top_k, embedding_field)
                queries.append(self._mtl(text_fields, routings))
            case _:
                raise ValueError("unknown similarity type")
        must_not = [Term(doc_id_field, self.doc_id)]
        return QueryComponent(should=queries, must_not=must_not)

    def _knns(
        self, vectors: Sequence[Vector], top_k: int, embedding_field: str
    ) -> list[Query]:
        queries = []
        for v in vectors:
            query = Knn(
                field=embedding_field,
                query_vector=v,
                boost=self.embedding_weight * 20,
                k=top_k * 5,
            )
            queries.append(query)
        return queries

    def _mtl(self, text_fields: list[str], routings: Collection[Routing]) -> Query:
        likes = [
            dict(LikeDocument(_id=chunk_id, routing=chunk_routing))
            for chunk_id, chunk_routing in routings
        ]
        mtl = MoreLikeThis(
            fields=text_fields,
            like=likes,
            boost=self.term_frequency_weight,
            min_term_freq=1,
            min_doc_freq=1,
        )
        return mtl


class LikeText(_ToES):
    text: str
    similarity: SimilarityType
    term_frequency_weight: float = 1.0
    embedding_weight: float = 1.0

    def to_es(
        self,
        top_k: int,
        vector: Vector | None = None,
        *,
        text_fields: list[str] | None = None,
        embedding_field: str | None = None,
    ) -> QueryComponent:
        # self._validate_args(
        #     self.similarity,
        #     vector,
        #     text_fields=text_fields,
        #     embedding_field=embedding_field,
        # )
        match self.similarity:
            case SimilarityType.TERM_FREQUENCY:
                queries = [self._mtl(text_fields)]
            case SimilarityType.EMBEDDING:
                if vector is None:
                    raise ValueError("missing vector")
                knn = self._knn(vector, top_k, embedding_field)
                queries = [knn]
            case SimilarityType.HYBRID:
                if vector is None:
                    raise ValueError("missing vector")
                mtl = self._mtl(text_fields)
                knn = self._knn(vector, top_k, embedding_field)
                queries = [mtl, knn]
            case _:
                raise ValueError("unknown similarity type")
        return QueryComponent(should=queries)

    def _knn(self, vector: Vector, top_k: int, embedding_field: str) -> Query:
        return Knn(
            field=embedding_field,
            query_vector=vector,
            boost=self.embedding_weight * 20,
            k=top_k * 5,
        )

    def _mtl(self, text_fields: list[str]) -> Query:
        mtl = MoreLikeThis(
            like=self.text,
            boost=self.term_frequency_weight,
            fields=text_fields,
            min_term_freq=1,
            min_doc_freq=1,
        )
        return mtl


# TODO: rename to keywords
class WithTerms(_ToES):
    terms: list[str]

    def to_es(self, fields: list[str]) -> QueryComponent:
        terms = " ".join(self.terms)
        queries = [
            Match(f, MatchQuery(query=terms, fuzziness=0, operator="and"))
            for f in fields
        ]
        return QueryComponent(filter=queries)


class UnlikeDoc(_ToES):
    # TODO: add support for semantic UnlikeDoc through _get_boostative boosting
    doc_id: str
    weight: float = 1.0

    def to_es(
        self, doc_id_field: str, text_fields: list[str], routings: Collection[Routing]
    ) -> QueryComponent:
        like = [
            dict(LikeDocument(_id=chunk_id, routing=chunk_routing))
            for chunk_id, chunk_routing in routings
        ]
        mtl = MoreLikeThis(
            like=like, min_term_freq=1, min_doc_freq=1, fields=text_fields
        )
        must_not = [Term(doc_id_field, self.doc_id)]
        return QueryComponent(script_score=[mtl], must_not=must_not)


class UnlikeText(_ToES):
    # TODO: add support for semantic UnlikeText through negative boosting
    text: str
    weight: float = 1.0

    def to_es(self, fields: list[str]) -> QueryComponent:
        like = [{"doc": dict.fromkeys(fields, self.text)}]
        mtl = MoreLikeThis(
            like=like, min_term_freq=1, min_doc_freq=1, boost=self.weight, fields=fields
        )
        return QueryComponent(script_score=[mtl])


class WithoutTerms(_ToES):
    terms: list[str]

    def to_es(self, fields: list[str]) -> QueryComponent:
        terms = " ".join(self.terms)
        must_not = [
            Match(f, MatchQuery(query=terms, fuzziness=0, operator="or"))
            for f in fields
        ]
        return QueryComponent(must_not=must_not)


# TODO: discriminated union ?
Like = LikeDoc | LikeText | WithTerms
Unlike = UnlikeDoc | UnlikeText | WithoutTerms


class SimilarDocSearch(BaseModel):
    like: list[Like] = Field(default_factory=list)
    unlike: list[Unlike] = Field(default_factory=list)

    def to_es(
        self,
        top_k: int,
        *,
        text_fields: list[str],
        doc_id_field: str,
        embedding_field: str,
        text_vectors: dict[str, Vector],
        doc_vectors: dict[str, Sequence[Vector]],
        doc_routings: dict[str, Collection[Routing]],
    ) -> Query:
        if not self.like:
            msg = (
                "like can't be empty, please provide similarity criteria before "
                "querying ES"
            )
            raise ValueError(msg)
        comps = self._components_to_es(
            top_k,
            doc_id_field=doc_id_field,
            text_fields=text_fields,
            embedding_field=embedding_field,
            text_vectors=text_vectors,
            doc_vectors=doc_vectors,
            doc_routings=doc_routings,
        )
        # We have to match at least one of the should query
        must = Bool(should=comps.should) or DEFAULT
        query = Bool(
            must=must,
            filter=comps.filter or DEFAULT,
            should=comps.unlike or DEFAULT,  # rescoring with unlike
            must_not=comps.must_not or DEFAULT,
        )
        return query

    def _components_to_es(
        self,
        top_k: int,
        *,
        text_fields: list[str],
        doc_id_field: str,
        embedding_field: str,
        text_vectors: dict[str, Vector],
        doc_vectors: dict[str, Sequence[Vector]],
        doc_routings: dict[str, Collection[Routing]],
    ) -> QueryComponent:
        like_doc = [
            q.to_es(
                top_k,
                doc_vectors[q.doc_id],
                doc_id_field=doc_id_field,
                routings=doc_routings[q.doc_id],
                text_fields=text_fields,
                embedding_field=embedding_field,
            )
            for q in self.like
            if isinstance(q, LikeDoc)
        ]
        like_text = [
            q.to_es(
                top_k,
                text_vectors[q.text],
                text_fields=text_fields,
                embedding_field=embedding_field,
            )
            for q in self.like
            if isinstance(q, LikeText)
        ]
        with_terms = [
            q.to_es(text_fields) for q in self.like if isinstance(q, WithTerms)
        ]
        unlike_doc = [
            q.to_es(doc_id_field, text_fields, doc_routings[q.doc_id])
            for q in self.unlike
            if isinstance(q, UnlikeDoc)
        ]
        unlike_text = [
            q.to_es(text_fields) for q in self.unlike if isinstance(q, UnlikeText)
        ]
        without_terms = [
            q.to_es(text_fields) for q in self.unlike if isinstance(q, WithoutTerms)
        ]
        comps = (
            like_doc + like_text + with_terms + unlike_doc + unlike_text + without_terms
        )
        return sum(comps, start=QueryComponent())

    def _must_not(
        self, doc_id_field: str, fields: list[str]
    ) -> list[Query] | DefaultType:
        filters = [q.to_es(fields) for q in self.like if isinstance(q, WithoutTerms)]
        filtered_ids = [q.doc_id for q in self.like if isinstance(q, LikeDoc)]
        if filtered_ids:
            filters.append([Term(doc_id_field, doc_id) for doc_id in filtered_ids])
        if not filters:
            return DEFAULT
        return sum(filters, start=[])

    def _should(
        self,
        top_k: int,
        *,
        text_fields: list[str] | None = None,
        embedding_field: str | None = None,
        text_vectors: dict[str, Vector],
        doc_vectors: dict[str, Sequence[Vector]],
        doc_routings: dict[str, Collection[Routing]],
    ) -> list[Query] | DefaultType:
        like_docs = (q for q in self.like if isinstance(q, LikeDoc))
        like_docs = [
            q.to_es(
                top_k,
                doc_vectors[q.doc_id],
                routings=doc_routings[q.doc_id],
                text_fields=text_fields,
                embedding_field=embedding_field,
            )
            for q in like_docs
        ]
        like_texts = (q for q in self.like if isinstance(q, LikeText))
        like_texts = [
            q.to_es(
                top_k,
                text_vectors[q.text],
                text_fields=text_fields,
                embedding_field=embedding_field,
            )
            for q in like_texts
        ]
        queries = like_docs + like_texts
        queries = sum(queries, start=[])
        if not queries:
            return DEFAULT
        return queries

    @property
    def is_empty(self) -> bool:
        return not (self.like or self.unlike)


def _sum_boosts(should: Query | list[Query]) -> float:
    if isinstance(should, DefaultType):
        return 1.0
    if isinstance(should, list):
        return sum(_get_boost(q.to_dict()) for q in should)
    return _get_boost(should.to_dict())


def _get_boost(q: dict) -> float:
    boost = q.get("boost")
    if boost is not None:
        return boost
    comps = [v for v in q.values() if isinstance(v, dict)]
    comps += [
        v for l in q.values() if isinstance(l, list) for v in l if isinstance(v, dict)
    ]
    boost = sum(_get_boost(v) for v in comps)
    return boost


class Retriever(ABC):
    @abstractmethod
    async def retrieve_similar(
        self, search: SimilarDocSearch, top_k: int = 100
    ) -> list[Document]: ...

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        return


class ESRetrieverConfig(BaseModel):
    index: str
    chunks_index: str
    dim: int = DEFAULT_EMBEDDING_DIM


class ESRetriever(Retriever):
    _DOC_SOURCES = [
        DOC_ROOT_ID,
        DOC_CONTENT,
        DOC_CONTENT_TYPE,
        DOC_PATH,
        DOC_LANGUAGE,
        DOC_EXTRACTION_LEVEL,
    ]
    _CHUNKS_SOURCE = [CHUNK_DOC_ID, CHUNK_EMBEDDING, CHUNK_ID]

    def __init__(
        self,
        client: ESClient,
        embedder: Embedder,
        chunker: "BaseChunker",
        config: ESRetrieverConfig,
    ) -> None:
        self._client = client
        self._embedder = embedder
        self._chunker = chunker

        self._config = config
        self._index = config.index
        self._chunk_index = config.chunks_index
        self.__n_docs: int | None = None
        self._doc_vectors: dict[str, list[Vector]] = dict()
        self._doc_routings: dict[str, set[Routing]] = dict()
        self._text_vectors: dict[str, Vector] = dict()
        self._routings: dict[str, str] = dict()

    @property
    def _n_docs(self) -> int:
        if self.__n_docs is None:
            raise ValueError("invalid state, call __aenter__ first")
        return self.__n_docs

    async def __aenter__(self) -> Self:
        self.__n_docs = await self._count_docs()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        self.__n_docs = None
        self._doc_vectors = dict()
        self._text_vectors = dict()
        gc.collect()

    async def retrieve_similar(
        self, search: SimilarDocSearch, top_k: int = 100
    ) -> list[Document]:
        # TODO: ideally we should use a PIT to retrieve docs and make sure top_k
        #  is consistent
        similar_docs = []
        scores = dict()
        await self._refresh_caches(search)
        query = search.to_es(
            top_k,
            text_fields=[CHUNK_TEXT],
            doc_id_field=CHUNK_DOC_ID,
            embedding_field=CHUNK_EMBEDDING,
            text_vectors=self._text_vectors,
            doc_vectors=self._doc_vectors,
            doc_routings=self._doc_routings,
        )
        query = {QUERY: query.to_dict()}
        search_after = None
        # We can fetch several chunks from the same doc, we preemptively fetch more docs
        top_k = min(top_k, self._n_docs)
        limit = min(2 * top_k, self._n_docs)
        search_factory = partial(
            self._client.poll_search_pages,
            index=self._chunk_index,
            sort=[SCORE_, CHUNK_DOC_ID, DOC_ROOT_ID],
            source=self._CHUNKS_SOURCE,
            track_scores=True,
        )
        # Fetch mix of docs and chunks until we have enough results
        while len(similar_docs) < top_k:
            # TODO: remove log
            logger.info("ES query: %s", query)
            if search_after is not None:
                query[SEARCH_AFTER] = search_after
            has_new_results = False
            search = search_factory(query, limit=limit)
            async for page in search:
                has_new_results = True
                page_hits = page[HITS][HITS]
                limit = min(page[HITS][TOTAL][VALUE], limit)
                for r in page_hits:
                    source = r[SOURCE]
                    if source.get("type") == ES_DOCUMENT_TYPE:
                        similar_docs.append(r)
                    doc_id = source.get(CHUNK_DOC_ID, r[ID_])
                    # For each doc only keep result with the highest score
                    if doc_id not in scores:
                        scores[doc_id] = r[SCORE_]
                    # Keep track of the ranking
                    search_after = page_hits[-1][SORT]
            if not has_new_results:
                break
        missing = set(scores) - {c[ID_] for c in similar_docs}
        async for doc_page in self._fetch_docs(missing):
            similar_docs.extend(doc_page[HITS][HITS])
        similar_docs = {c[ID_]: c for c in similar_docs}
        similar_docs = sorted(
            similar_docs.items(), key=lambda i: scores[i[0]], reverse=True
        )
        similar_docs = similar_docs[:top_k]
        similar_docs = (c for _, c in similar_docs)
        similar_docs = [Document.from_es(c) for c in similar_docs]
        return similar_docs

    async def _fetch_docs(
        self, doc_ids: Collection[str]
    ) -> AsyncIterable[dict[str, Any]]:
        query = {QUERY: Ids(values=list(set(doc_ids))).to_dict()}
        async for page in self._client.poll_search_pages(
            query, index=self._index, source=self._DOC_SOURCES, sort=DOC_ROOT_ID
        ):
            yield page

    async def _count_docs(self) -> int:
        query = {QUERY: Term("type", ES_DOCUMENT_TYPE).to_dict()}
        count = await self._client.count(query, index=self._index)
        return count[COUNT]

    async def _refresh_caches(self, search: SimilarDocSearch) -> None:
        tasks = [
            self._refresh_doc_vectors_and_chunk_ids(search),
            self._refresh_text_vectors(search),
        ]
        await asyncio.gather(*tasks)

    async def _refresh_doc_vectors_and_chunk_ids(
        self, search: SimilarDocSearch
    ) -> None:
        doc_vectors = defaultdict(list, self._doc_vectors)
        routings = defaultdict(set, self._doc_routings)
        to_vectorize = {
            like.doc_id
            for like in search.like
            if isinstance(like, LikeDoc) and like.doc_id not in doc_vectors
        }
        to_route = {
            like.doc_id
            for like in search.unlike
            if isinstance(like, UnlikeDoc) and like.doc_id not in routings
        }
        to_retrieve = to_vectorize.union(to_route)
        if not to_retrieve:
            return
        # TODO: ideally we should use routing...
        # TODO: poll concurrently
        query = Bool(should=[Term(CHUNK_DOC_ID, doc_id) for doc_id in to_retrieve])
        query = {QUERY: query.to_dict()}
        search = self._client.poll_search_pages(
            query, CHUNK_ID, index=self._chunk_index, source=self._CHUNKS_SOURCE
        )
        async for page in search:
            for chunk in page[HITS][HITS]:
                routing = (chunk[ID_], chunk[ROUTING_])
                chunk = chunk[SOURCE]  # noqa: PLW2901
                doc_id = chunk[CHUNK_DOC_ID]
                routings[doc_id].add(routing)
                if doc_id in to_vectorize:
                    doc_vectors[doc_id].append(chunk[CHUNK_EMBEDDING])
        self._doc_routings = dict(routings)
        self._doc_vectors = dict(doc_vectors)

    async def _refresh_text_vectors(self, search: SimilarDocSearch) -> None:
        text_vectors = dict(self._text_vectors)
        to_vectorize = {
            like.text
            for like in search.like
            if isinstance(like, LikeText) and like.text not in text_vectors
        }
        if not to_vectorize:
            return
        to_vectorize = list(to_vectorize)
        chunks = []
        for text in to_vectorize:
            chunk = list(chunk_text(text, self._chunker, sep_pattern=None))
            if len(chunk) != 1:
                raise ValueError("expected single chunk")
            chunks.append(chunk[0])
        texts = [self._chunker.contextualize(chunk) for chunk in chunks]
        embeddings = await self._embedder.embed(texts, self._config.dim)
        for text, embedding in zip(to_vectorize, embeddings, strict=True):
            text_vectors[text] = embedding
        self._text_vectors = dict(text_vectors)
