import re
from collections import defaultdict
from collections.abc import AsyncIterable, Callable
from enum import StrEnum, unique
from hashlib import md5
from typing import Annotated, ClassVar, Protocol, Self

from datashare_python.objects import Document as _Document
from icij_common.es import (
    DOC_CONTENT,
    DOC_CONTENT_LENGTH,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    ID_,
    SOURCE,
)
from icij_common.pydantic_utils import (
    icij_config,
    lowercamel_case_config,
    make_enum_discriminator,
    merge_configs,
    no_enum_values_config,
    safe_copy,
)
from pydantic import BaseModel as _BaseModel
from pydantic import Discriminator, Field, Tag, model_validator

from .utils_ import lower_camel_to_snake_case


class TaskQueues(StrEnum):
    CPU = "ner-cpu"


class BaseModel(_BaseModel):
    model_config = merge_configs(icij_config(), no_enum_values_config())


class DatashareModel(BaseModel):
    model_config = merge_configs(BaseModel.model_config, lowercamel_case_config())


class BatchDocument(DatashareModel):
    id: str
    root_document: str
    project: str
    language: str

    @classmethod
    def from_es(cls, es_doc: dict, project: str) -> Self:
        sources = es_doc[SOURCE]
        return cls(
            project=project,
            id=es_doc[ID_],
            root_document=sources[DOC_ROOT_ID],
            language=sources[DOC_LANGUAGE],
        )


class Document(_Document):
    content: str
    content_length: int
    project: str

    @classmethod
    def from_es(cls, es_doc: dict, project: str) -> Self:
        sources = es_doc[SOURCE]
        return cls(
            project=project,
            id=es_doc[ID_],
            root_document=sources[DOC_ROOT_ID],
            language=sources[DOC_LANGUAGE],
            content=sources[DOC_CONTENT],
            content_length=sources[DOC_CONTENT_LENGTH],
        )


@unique
class Category(StrEnum):
    PER = "PERSON"
    ORG = "ORGANIZATION"
    LOC = "LOCATION"
    DATE = "DATE"
    MONEY = "MONEY"
    NUM = "NUMBER"
    UNK = "UNKNOWN"


@unique
class NER(StrEnum):
    SPACY = "SPACY"
    TEST = "TEST"


@unique
class SpacySize(StrEnum):
    SMALL = "sm"
    MEDIUM = "md"
    LARGE = "lg"
    TRANSFORMER = "trf"


class TestConfig(DatashareModel):
    type: Annotated[ClassVar[NER], Field(frozen=True)] = NER.TEST


class SpacyConfig(DatashareModel):
    type: Annotated[ClassVar[NER], Field(frozen=True)] = NER.SPACY
    model_size: SpacySize = SpacySize.SMALL


# TODO: move this pydantic utils
def tagged_union(
    members: tuple[type, ...], tag_getter: Callable[[type], str]
) -> Annotated[type, Tag] | ...:
    if not members:
        raise ValueError("empty members")
    first = members[0]
    tagged = Annotated[first, Tag(tag_getter(first))]
    for m in members[1:]:
        tagged |= Annotated[m, Tag(tag_getter(m))]
    return tagged


NERConfig = tagged_union((SpacyConfig, TestConfig), lambda x: x.type.value)


class NERPayload(DatashareModel):
    docs: list[Document]
    categories: list[Category] = [Category.LOC, Category.PER, Category.ORG]
    config: Annotated[
        NERConfig,
        Field(
            default_factory=SpacyConfig,
            discriminator=Discriminator(make_enum_discriminator("type", NER)),
        ),
    ]


class NlpTag(BaseModel):
    start: int
    mention: str
    category: Category

    def with_offset(self, offset: int) -> Self:
        return safe_copy(self, update={"start": self.start + offset})


class NERTagger(Protocol):
    def __call__(
        self, texts: list[str], categories: set[Category] | None = None
    ) -> AsyncIterable[list[NlpTag]]: ...


_ID_PLACEHOLDER, _MENTION_NORM_PLACEHOLDER = "", ""


class NamedEntity(DatashareModel):
    id: str = _ID_PLACEHOLDER
    category: Category
    mention: str
    mention_norm: str = _ID_PLACEHOLDER
    document_id: str
    root_document: str
    extractor: str
    type: str = Field(default="NamedEntity", frozen=True)
    extractor_language: str
    offsets: list[int]

    @model_validator(mode="before")
    @classmethod
    def generate_id(cls, values: dict) -> dict:
        values = {lower_camel_to_snake_case(k): v for k, v in values.items()}
        provided_mention_norm = values.get("mention_norm")
        hasher = md5()
        offsets = values.get("offsets", [])
        offsets = sorted(offsets)
        mention = values.get("mention", "")
        mention_norm = _normalize(mention)
        if provided_mention_norm is not None and provided_mention_norm != mention_norm:
            msg = (
                f'provided mention_norm ("{provided_mention_norm}") differs from '
                f'computed mention norm ("{mention_norm}")'
            )
            raise ValueError(msg)
        values["mention_norm"] = mention_norm
        doc_id = values.get("document_id", "")
        hashed = (doc_id, str(offsets), values["extractor"], mention_norm)
        for h in hashed:
            hasher.update(h.encode())
        ne_id = hasher.hexdigest()
        provided_id = values.get("id")
        if provided_id is not None and provided_id != ne_id:
            msg = f'provided id ("{provided_id}") differs from computed id ("{ne_id}")'
            raise ValueError(msg)
        values["id"] = ne_id
        return values

    @classmethod
    def from_tags(
        cls, tags: list[NlpTag], doc: BatchDocument, extractor: NER
    ) -> list[Self]:
        entities = defaultdict(list)
        for tag in tags:
            entities[(tag.mention, tag.category)].append(tag.start)
        ents = []
        for (mention, category), offsets in entities.items():
            ents.append(
                NamedEntity(
                    category=category,
                    mention=mention,
                    document_id=doc.id,
                    root_document=doc.root_document,
                    extractor_language=doc.language,
                    offsets=offsets,
                    extractor=extractor,
                )
            )
        return ents


_CONSECUTIVE_SPACES_RE = re.compile(r"\s+")


def _normalize(s: str) -> str:
    s = s.strip()
    return _CONSECUTIVE_SPACES_RE.sub(" ", s).lower()
