from abc import ABC, abstractmethod
from collections.abc import Sequence
from datashare_python.objects import BaseModel
from elastic_transport._models import DEFAULT, DefaultType
from elasticsearch.dsl import Query
from elasticsearch.dsl.query import Bool, Knn, MoreLikeThis, Term, Terms
from elasticsearch.dsl.types import LikeDocument
from enum import StrEnum
from icij_common.es import QUERY
from pydantic import Field, model_validator
from typing import Any, Self

from .objects import Vector

_SCORE = "_score"


class SimilarityType(StrEnum):
    TERM_FREQUENCY = "term_frequency"
    EMBEDDING = "embedding"
    HYBRID = "hybrid"


class _ToES(BaseModel, ABC):
    @abstractmethod
    def to_es(self, **kwargs) -> list[Query]: ...


class LikeDoc(_ToES):
    doc_id: str
    text_index: str
    embedding_index: str
    text_fields: list[str] = Field(default_factory=list)
    embedding_fields: list[str] = Field(default_factory=list)
    similarity: SimilarityType
    text_boost: float = 1.0
    embedding_boost: float = 1.0

    @model_validator(mode="after")
    def _validate_has_fields(self) -> Self:
        require_text = (
            self.similarity is SimilarityType.TERM_FREQUENCY
            or self.similarity is SimilarityType.HYBRID
        )
        require_embedding = (
            self.similarity is SimilarityType.EMBEDDING
            or self.similarity is SimilarityType.HYBRID
        )
        errors = []
        if require_text and not self.text_fields:
            errors.index("text fields")
        if require_embedding and not self.embedding_fields:
            errors.index("embedded fields")
        if errors:
            msg = " and ".join(errors) + " are missing"
            raise ValueError(msg)
        return self

    def to_es(self, top_k: int, vectors: Sequence[Vector] | None = None) -> list[Query]:
        match self.similarity:
            case SimilarityType.TERM_FREQUENCY:
                queries = [self._mtl()]
            case SimilarityType.EMBEDDING:
                if vectors is None:
                    raise ValueError("missing vector")
                queries = self._knns(vectors, top_k=top_k)
            case SimilarityType.HYBRID:
                if vectors is None:
                    raise ValueError("missing vector")
                queries = self._knns(vectors, top_k=top_k)
                queries.append(self._mtl())
            case _:
                raise ValueError("unknown similarity type")
        return queries

    def _knns(self, vectors: Sequence[Vector], top_k: int) -> list[Query]:
        queries = []
        for v in vectors:
            query = Knn(
                fields=self.embedding_fields,
                query_vector=v,
                boost=self.embedding_boost,
                k=top_k,
                filter=Term(_field="_index", _index=self.embedding_index),
            )
            queries.append(query)
        return queries

    def _mtl(self) -> "Query":
        from elasticsearch.dsl.types import LikeDocument  # noqa: PLC0415

        mtl = MoreLikeThis(
            fields=self.text_fields,
            like=LikeDocument(_id=self.doc_id, _index=self.text_index),
            boost=self.text_boost,
            min_term_freq=1,
        )
        return mtl


class LikeText(_ToES):
    text: str
    text_index: str
    embedding_index: str
    text_fields: list[str]
    embedding_fields: list[str]
    similarity: SimilarityType
    text_boost: float = 1.0
    embedding_boost: float = 1.0

    @model_validator(mode="after")
    def _validate_has_fields(self) -> Self:
        require_text = (
            self.similarity is SimilarityType.TERM_FREQUENCY
            or self.similarity is SimilarityType.HYBRID
        )
        require_embedding = (
            self.similarity is SimilarityType.EMBEDDING
            or self.similarity is SimilarityType.HYBRID
        )
        errors = []
        if require_text and not self.text_fields:
            errors.index("text fields")
        if require_embedding and not self.embedding_fields:
            errors.index("embedded fields")
        if errors:
            msg = " and ".join(errors) + " are missing"
            raise ValueError(msg)
        return self

    def to_es(self, top_k: int, vector: Vector | None = None) -> list[Query]:
        match self.similarity:
            case SimilarityType.TERM_FREQUENCY:
                queries = [self._mtl()]
            case SimilarityType.EMBEDDING:
                if vector is None:
                    raise ValueError("missing vector")
                knn = self._knn(vector, top_k=top_k)
                queries = [knn]
            case SimilarityType.HYBRID:
                if vector is None:
                    raise ValueError("missing vector")
                queries = [self._mtl(), self._knn(vector, top_k=top_k)]
            case _:
                raise ValueError("unknown similarity type")
        return queries

    def _knn(self, vector: Vector, top_k: int) -> "Query":
        return Knn(
            fields=self.embedding_fields,
            query_vector=vector,
            boost=self.embedding_boost,
            k=top_k,
            filter=Term(_field="_index", _index=self.embedding_index),
        )

    def _mtl(self) -> Query:
        mtl = MoreLikeThis(
            like=self.text,
            boost=self.text_boost,
            fields=self.text_fields,
            min_term_freq=1,
        )
        return mtl


# TODO: rename to keywords
class WithTerms(_ToES):
    terms: list[str]
    field: str
    boost: float = 1.0

    def to_es(self) -> list[Query]:
        return [Terms(_field=self.field, terms=self.terms, boost=self.boost)]


class UnlikeDoc(_ToES):
    # TODO: add support for semantic UnlikeDoc through negative boosting
    doc_id: str
    index: str
    fields: list[str]
    boost: float = 1.0

    def to_es(self) -> list[Query]:
        mtl = MoreLikeThis(
            fields=self.fields,
            unlike=LikeDocument(_id=self.doc_id, _index=self.index),
            boost=self.boost,
            min_term_freq=1,
        )
        return [mtl]


class UnlikeText(_ToES):
    # TODO: add support for semantic UnlikeText through negative boosting
    text: str
    index: str
    fields: list[str]
    boost: float = 1.0

    def to_es(self) -> list[Query]:
        like = {"_index": self.index, "doc": dict.fromkeys(self.fields, self.text)}
        mtl = MoreLikeThis(like=like, boost=self.boost, min_term_freq=1)
        return [mtl]


class WithoutTerms(_ToES):
    terms: list[str]
    field: str
    boost: float = 1.0

    def to_es(self) -> list[Query]:
        return [
            Bool(must_not=Terms(_field=self.field, terms=self.terms), boost=self.boost)
        ]


# TODO: discriminated union ?
Like = LikeDoc | LikeText | WithTerms
Unlike = UnlikeDoc | UnlikeText | WithoutTerms


class SimilarDocSearch(BaseModel):
    use_vectors: bool

    like: list[Like] = Field(default_factory=list)
    unlike: list[Unlike] = Field(default_factory=list)

    def to_es(
        self,
        top_k: int,
        *,
        text_vectors: dict[str, Vector],
        doc_vectors: dict[str, Sequence[Vector]],
    ) -> Query:
        should = self._should(top_k, text_vectors=text_vectors, doc_vectors=doc_vectors)
        query = Bool(filter=self._filters, must_not=self._must_not, should=should)
        query = {QUERY: query.to_dict()}
        return query

    @property
    def _filters(self) -> list[Query] | DefaultType:
        filters = [q.to_es() for q in self.like if isinstance(q, WithTerms)]
        if not filters:
            return DEFAULT
        return sum(filters, start=[])

    @property
    def _must_not(self) -> list[Query] | DefaultType:
        filters = [q.to_es() for q in self.unlike if isinstance(q, WithoutTerms)]
        if not filters:
            return DEFAULT
        return sum(filters, start=[])

    def _should(
        self,
        top_k: int,
        *,
        text_vectors: dict[str, Vector],
        doc_vectors: dict[str, Sequence[Vector]],
    ) -> list[Query] | DefaultType:
        like_docs = [
            q.to_es(top_k, doc_vectors[q.doc_id])
            for q in self.like
            if isinstance(q, LikeDoc)
        ]
        like_texts = [
            q.to_es(top_k, text_vectors[q.text])
            for q in self.like
            if isinstance(q, LikeText)
        ]
        unlike_docs = [q.to_es() for q in self.unlike if isinstance(q, UnlikeDoc)]
        unlike_texts = [q.to_es() for q in self.unlike if isinstance(q, UnlikeText)]
        queries = like_docs + like_texts + unlike_docs + unlike_texts
        queries = sum(queries, start=[])
        if not queries:
            return DEFAULT
        return queries

    @property
    def is_empty(self) -> bool:
        return not (self.like or self.unlike)


class SimilarDocResponse(BaseModel):
    docs: list[dict]
    pit: dict[str, Any]
