from collections import defaultdict

import asyncio
import io
import json
import logging
import questionary
import shutil
import sys
import typer
from collections.abc import Sequence
from dataclasses import dataclass
from docling_core.transforms.chunker import BaseChunker
from elasticsearch.dsl.query import Terms
from enum import StrEnum
from icij_common.es import (
    DOC_CONTENT,
    DOC_CONTENT_TYPE,
    DOC_PATH,
    DOC_ROOT_ID,
    ESClient,
    HITS,
    ID_,
    INDEX_,
    PIT_ID,
    QUERY,
    SOURCE,
)
from icij_common.pydantic_utils import safe_copy
from prompt_toolkit import ANSI
from prompt_toolkit.formatted_text import to_formatted_text
from pydantic import ValidationError
from questionary import Question
from rich import box
from rich.console import Console
from rich.table import Column, Table
from typing import Annotated, Any

from .utils import AsyncTyper
from ..config import ESClientConfig, ESClientSetting, EmbeddingWorkerConfig
from ..constants import (
    CHUNK_DOC_ID,
    CHUNK_EMBEDDING,
    DEFAULT_EMBEDDING_DIM,
    DEFAULT_EMBEDDING_MODEL,
)
from ..embeddings import Embedder, HFEmbedder, chunk_text, make_chunker
from ..objects import Vector
from ..search import (
    Like,
    LikeDoc,
    LikeText,
    SimilarDocSearch,
    SimilarityType,
    Unlike,
    UnlikeDoc,
    UnlikeText,
    WithTerms,
    WithoutTerms,
)

_SEARCH = "search"

logger = logging.getLogger(__name__)

search_app = AsyncTyper(name=_SEARCH)

DocID = str

_SEARCH_SIMILAR_DOCS_HELP = "search similar documents"
_SIMILAR_DOCS_SEARCH_HELP = "similar document search query from a previous session"
_SIMILAR_DOCS_ES_HELP = (
    "elasticsearch URL or ESClientConfig as JSON. "
    "By default this will use the worker config from env."
)
_SIMILAR_DOCS_DISABLE_VECTORS_HELP = "disable embedding usage"
_SIMILAR_DOCS_HELP = "number of similar documents. If provided, search take precedence"
_SIMILAR_DOCS_PROJECT_HELP = "project name. If provided, search take precedence"
_SIMILAR_DOCS_EMBEDDER_CONFIG = "embedder config as a JSON string or path to JSON file"
_SIMILAR_DOCS_MODEL_HELP = "HF embedding model ID"
_SIMILAR_DOCS_MODEL_REVISION_HELP = "HF embedding model revision"
_SIMILAR_DOCS_DIM_HELP = "embedding dimension"
_SIMILAR_DOCS_DS_URL_HELP = "datashare URL, used to generate links to documents"


@search_app.async_command(help=_SEARCH_SIMILAR_DOCS_HELP)
async def similar_docs(  # noqa: PLR0917
    project: Annotated[str, typer.Argument(help=_SIMILAR_DOCS_PROJECT_HELP)],
    search: Annotated[str | None, typer.Option(help=_SIMILAR_DOCS_SEARCH_HELP)] = None,
    model: Annotated[
        str, typer.Option(help=_SIMILAR_DOCS_MODEL_HELP)
    ] = DEFAULT_EMBEDDING_MODEL,
    model_revision: Annotated[
        str | None, typer.Option(help=_SIMILAR_DOCS_MODEL_REVISION_HELP)
    ] = None,
    dim: Annotated[
        int, typer.Option("--dim", help=_SIMILAR_DOCS_DIM_HELP)
    ] = DEFAULT_EMBEDDING_DIM,
    # TODO: make this one optional and do not generate links when not provided
    ds_url: Annotated[
        str, typer.Option("--ds-url", "--ds", help=_SIMILAR_DOCS_DS_URL_HELP)
    ] = "http://localhost:8080",
    elasticsearch: Annotated[
        str | None, typer.Option("--es", "--elasiticserach", help=_SIMILAR_DOCS_ES_HELP)
    ] = None,
    *,
    disable_vectors: Annotated[
        bool,
        typer.Option("--disable-vectors", help=_SIMILAR_DOCS_DISABLE_VECTORS_HELP),
    ] = False,
    embedding_config: Annotated[
        str | None,
        typer.Option("-c", "--embedding-config", help=_SIMILAR_DOCS_EMBEDDER_CONFIG),
    ] = None,
    n: Annotated[
        int,
        typer.Option("-n", help=_SIMILAR_DOCS_HELP),
    ] = 100,
) -> None:
    if search is not None:
        search = SimilarDocSearch.model_validate_json(search)
    else:
        search = SimilarDocSearch(use_vectors=not disable_vectors)
    if embedding_config is None:
        embedding_config = EmbeddingWorkerConfig()
    if elasticsearch is None:
        elasticsearch = ESClientSetting()
    else:
        try:
            elasticsearch = ESClientConfig.model_validate_json(elasticsearch)
        except ValidationError:
            elasticsearch = ESClientSetting(address=elasticsearch)
    es_client = elasticsearch.to_es_client()
    args = SimilarityArgs(
        ds_url=ds_url,
        model=model,
        model_revision=model_revision,
        dim=dim,
        n_candidates=n,
    )
    await _similar_docs_loop(es_client, embedding_config, args, project, search)


_EMBEDDING_FIELDS = [CHUNK_EMBEDDING]
_TEXT_FIELDS = [DOC_CONTENT]


@dataclass(frozen=True)
class SimilarityArgs:
    ds_url: str
    model: str
    model_revision: str | None
    dim: int
    n_candidates: int


class _StepAction(StrEnum):
    LIKE = "like"
    UNLIKE = "unlike"
    DISPLAY_RESULTS = "display_results"
    EDIT_SEARCH = "edit"
    VALIDATE = "validate"
    END = "end"


class _LikeTypes(StrEnum):
    DOC = "doc"
    TEXT = "text"
    # TODO: rename to keywaords
    WITH_TERMS = "with_terms"


class _DocAction(StrEnum):
    BY_ID = "by_id"
    FROM_CANDIDATES = "from_candidates"
    DONE = "exit"


class _TextAction(StrEnum):
    ADD = "add"
    DONE = "done"


class _Pagination(StrEnum):
    NEXT = "next"
    PREVIOUS = "previous"
    SAME = "same"
    DONE = "done"


async def _similar_docs_loop(
    client: ESClientSetting,
    config: EmbeddingWorkerConfig,
    args: SimilarityArgs,
    project: str,
    search: SimilarDocSearch,
) -> None:
    text_index = project
    embedding_index = config.index_name(project)
    async with client.pit(index=[text_index, embedding_index], keep_alive="10m") as pit:
        # TODO: add incremental validation
        candidates = []
        doc_vectors, text_vectors = dict(), dict()
        model = args.model
        model_revision = args.model_revision
        ds_url = args.ds_url
        dim = args.dim
        embedder = HFEmbedder(config, model=model, model_revision=model_revision)
        chunker = make_chunker(config, model=model, model_revision=model_revision)
        try:
            while True:
                if not search.is_empty:
                    pit, candidates = await _search_similar_docs(
                        client,
                        search,
                        pit,
                        limit=args.n_candidates,
                        doc_vectors=doc_vectors,
                        text_vectors=text_vectors,
                    )
                    await _display_candidates(candidates, ds_url)
                search = await _similar_doc_step(
                    search,
                    candidates,
                    ds_url=ds_url,
                    text_index=project,
                    embedding_index=embedding_index,
                )
                tasks = [
                    _update_doc_vectors(client, doc_vectors, search),
                    _update_text_vectors(embedder, chunker, text_vectors, search, dim),
                ]
                res = await asyncio.gather(*tasks)
                doc_vectors, text_vectors = res
        finally:
            if search is not None:
                print(json.dumps(search.model_dump(), indent=2, sort_keys=True))


def _select(*args: Any, **kwargs: Any) -> Question:
    return questionary.select(*args, use_shortcuts=True, **kwargs)


async def _similar_doc_step(
    search: SimilarDocSearch,
    candidates: list[dict],
    *,
    ds_url: str,
    text_index: str,
    embedding_index: str,
) -> SimilarDocSearch:
    actions = await _prompt_action()
    like, unlike = [], []
    match actions:
        case _StepAction.LIKE:
            like = await _prompt_like(
                candidates,
                ds_url=ds_url,
                text_index=text_index,
                embedding_index=embedding_index,
            )
        case _StepAction.UNLIKE:
            unlike = await _prompt_unlike(candidates, ds_url=ds_url, index=text_index)
        case _StepAction.EDIT_SEARCH:
            raise NotImplementedError("to be implemented")
        case _StepAction.VALIDATE:
            raise NotImplementedError("to be implemented")
        case _StepAction.DISPLAY_RESULTS:
            await _display_candidates(candidates, ds_url)
            return search
        case _StepAction.END:
            raise typer.Exit()
        case _:
            raise ValueError(f"invalid action: {actions}")
    updated = {"like": search.like + like, "unlike": search.like + unlike}
    search = safe_copy(search, update=updated)
    return search


_DOC_SOURCES = [DOC_ROOT_ID, DOC_CONTENT, DOC_CONTENT_TYPE, DOC_PATH]


async def _search_similar_docs(
    client: ESClient,
    search: SimilarDocSearch,
    pit: dict,
    *,
    doc_vectors: dict[str, list[Vector]],
    text_vectors: dict[str, Vector],
    limit: int,
) -> tuple[dict, list[dict]]:
    # TODO: poll concurrently
    candidates = []
    query = search.to_es(limit, text_vectors=text_vectors, doc_vectors=doc_vectors)
    logger.info("ES query: %s", query)
    async for page in client.poll_search_pages(
        query, source=_DOC_SOURCES, pit=pit, limit=limit
    ):
        pit = page[PIT_ID]
        candidates.extend(d for d in page[HITS][HITS])
    return pit, candidates


_ACTIONS = [
    questionary.Choice("find docs like", value=_StepAction.LIKE),
    questionary.Choice("find docs unlike", value=_StepAction.UNLIKE),
    questionary.Choice("edit current search", value=_StepAction.EDIT_SEARCH),
    questionary.Choice("validate result", value=_StepAction.VALIDATE),
    questionary.Choice("display results", value=_StepAction.DISPLAY_RESULTS),
    questionary.Choice("terminate now", value=_StepAction.END),
]


async def _prompt_action() -> _StepAction:
    direction = await _select(
        "Select an action:", _ACTIONS, default=_StepAction.LIKE
    ).ask_async()
    return direction


_LIKE_TYPES = [
    questionary.Choice("similar to another doc", value=_LikeTypes.DOC),
    questionary.Choice("similar to text", value=_LikeTypes.TEXT),
    questionary.Choice("must contains keywords", value=_LikeTypes.WITH_TERMS),
]


async def _prompt_like(
    candidates: list[dict], *, ds_url: str, text_index: str, embedding_index: str
) -> list[Like]:
    like_type = await _select(
        "Similarity criteria:", _LIKE_TYPES, default=_LikeTypes.DOC
    ).ask_async()

    match like_type:
        case _LikeTypes.DOC:
            likes = await _prompt_like_docs(
                candidates,
                ds_url=ds_url,
                text_index=text_index,
                embedding_index=embedding_index,
            )
        case _LikeTypes.TEXT:
            likes = await _prompt_like_texts(
                text_index=text_index, embedding_index=embedding_index
            )
        case _LikeTypes.WITH_TERMS:
            boost = await _prompt_boost("Boost")
            terms = await _prompt_with_terms("keyword")
            likes = [WithTerms(terms=terms, field=DOC_CONTENT, boost=boost)]
        case _:
            raise ValueError(f"unexpected similarity type {like_type}")
    return likes


_UNLIKE_TYPES = [
    questionary.Choice("different from another doc", value=_LikeTypes.DOC),
    questionary.Choice("different from text", value=_LikeTypes.TEXT),
    questionary.Choice("must not contains keywords", value=_LikeTypes.WITH_TERMS),
]


async def _prompt_unlike(
    candidates: list[dict], *, ds_url: str, index: str
) -> list[Unlike]:
    like_type = _select(
        "Dissimilarity criteria:", _UNLIKE_TYPES, default=_LikeTypes.DOC
    )
    like_type = await like_type.ask_async()
    match like_type:
        case _LikeTypes.DOC:
            likes = await _prompt_unlike_docs(candidates, index=index, ds_url=ds_url)
        case _LikeTypes.TEXT:
            likes = await _prompt_unlike_texts(index=index)
        case _LikeTypes.WITH_TERMS:
            boost = await _prompt_boost("Boost")
            terms = await _prompt_with_terms("negative keyword")
            likes = [WithoutTerms(terms=terms, field=DOC_CONTENT, boost=boost)]
        case _:
            raise ValueError(f"unexpected similarity type {like_type}")
    return likes


_DOC_ACTIONS = [
    questionary.Choice("add by ID", value=_DocAction.BY_ID),
    questionary.Choice("add from results", value=_DocAction.FROM_CANDIDATES),
    questionary.Choice("done", value=_DocAction.DONE),
]


async def _prompt_like_docs(
    candidates: list[dict],
    *,
    ds_url: str,
    text_index: str,
    embedding_index: str,
) -> list[LikeDoc]:
    doc_ids = []
    default = _DocAction.FROM_CANDIDATES if candidates else _DocAction.BY_ID
    action = _select("Action:", _DOC_ACTIONS, default)
    act = await action.ask_async()
    if act is _DocAction.DONE:
        return []
    doc_ids += await _select_docs(candidates, act, ds_url)
    while (
        act := await _select("Action:", _DOC_ACTIONS, _DocAction.DONE).ask_async()
    ) is not _DocAction.DONE:
        doc_ids += await _select_docs(candidates, act, ds_url)
    similarity, text_boost, embedding_boost = await _prompt_similarity_type()
    likes = []
    for doc_id in doc_ids:
        like = LikeDoc(
            doc_id=doc_id,
            text_index=text_index,
            embedding_index=embedding_index,
            text_fields=_TEXT_FIELDS,
            embedding_fields=_EMBEDDING_FIELDS,
            similarity=similarity,
            text_boost=text_boost,
            embedding_boost=embedding_boost,
        )
        likes.append(like)
    return likes


async def _prompt_unlike_docs(
    candidates: list[dict], *, index: str, ds_url: str
) -> list[UnlikeDoc]:
    doc_ids = []
    default = _DocAction.FROM_CANDIDATES if candidates else _DocAction.BY_ID
    action = _select("Action:", _DOC_ACTIONS, default)
    act = await action.ask_async()
    if act is _DocAction.DONE:
        return []
    doc_ids += await _select_docs(candidates, act, ds_url)
    while (
        act := await _select("Next action:", _DOC_ACTIONS, default).ask_async()
    ) is not _DocAction.DONE:
        doc_ids += await _select_docs(candidates, act, ds_url)
    boost = _prompt_boost("Boost")
    likes = []
    for doc_id in doc_ids:
        like = UnlikeDoc(doc_id=doc_id, index=index, boost=boost, fields=_TEXT_FIELDS)
        likes.append(like)
    return likes


_TEXT_ACTIONS = [
    questionary.Choice("add another text", value=_TextAction.ADD),
    questionary.Choice("done", value=_TextAction.DONE),
]
_TEXT_ACTION_SELECT = _select("Action:", _TEXT_ACTIONS, _TextAction.DONE)


async def _prompt_like_texts(
    *, text_index: str, embedding_index: str
) -> list[LikeText]:
    texts = []
    texts.append(await _prompt_target_text())
    while (await _TEXT_ACTION_SELECT.ask_async()) is not _TextAction.DONE:
        texts.append(await _prompt_target_text())
    similarity, text_boost, embedding_boost = await _prompt_similarity_type()
    likes = []
    for text in texts:
        like = LikeText(
            text=text,
            text_index=text_index,
            embedding_index=embedding_index,
            text_fields=_TEXT_FIELDS,
            embedding_fields=_EMBEDDING_FIELDS,
            similarity=similarity,
            text_boost=text_boost,
            embedding_boost=embedding_boost,
        )
        likes.append(like)
    return likes


async def _prompt_unlike_texts(index: str) -> list[UnlikeText]:
    texts = []
    action = await _TEXT_ACTION_SELECT.ask_async()
    if action is _TextAction.DONE:
        return []
    texts.append(_prompt_target_text())
    while (await _TEXT_ACTION_SELECT.ask_async()) is not _TextAction.DONE:
        texts.append(await _prompt_target_text())
    likes = []
    for text in texts:
        like = UnlikeText(text=text, fields=_TEXT_FIELDS, index=index)
        likes.append(like)
    return likes


async def _prompt_with_terms(label: str) -> list[str]:
    terms = [await _prompt_term()]
    while await _prompt_add_new(label):
        likes = await _prompt_term()
        terms.append(likes)
    return terms


def _validate_doc_id(value: str) -> Any:
    if not value:
        return "empty doc ID"
    return True


_DOC_ID_PROMPT = questionary.text("Enter a doc ID: ", validate=_validate_doc_id)


async def _select_docs(
    candidates: list[dict], selection_type: _DocAction, ds_url: str
) -> list[str]:
    match selection_type:
        case _DocAction.BY_ID:
            doc_ids = await _DOC_ID_PROMPT.ask_async()
            doc_ids = [doc_ids]
        case _DocAction.FROM_CANDIDATES:
            candidates = await _select_candidates(candidates, ds_url)
            doc_ids = [d[ID_] for d in candidates]
        case _:
            raise ValueError(f"unexpected selection type {selection_type}")
    return doc_ids


_NEXT_PAGE = questionary.Choice("next page", value=_Pagination.NEXT)
_PREVIOUS_PAGE = questionary.Choice("previous page", value=_Pagination.PREVIOUS)
_DONE = questionary.Choice("done", value=_Pagination.DONE)
_SAME_PAGE = questionary.Choice("same page", value=_Pagination.SAME)


_DOC_COLS = [
    Column(header="Rank", overflow="ellipsis", ratio=1, no_wrap=True),
    Column(header="Doc ID", overflow="ellipsis", ratio=2),
    Column(header="Content Type", overflow="ellipsis", ratio=1, no_wrap=True),
    Column(header="Content", overflow="ellipsis", ratio=3),
]


async def _display_candidates(
    candidates: list[dict], ds_url: str, page_size: int = 20
) -> None:
    n_candidates = len(candidates)
    last = n_candidates // page_size
    current = 0
    while True:
        start = current * page_size
        end = min(n_candidates, start + page_size)
        docs = [candidates[i] for i in range(start, end)]
        rows = [
            _doc_to_row(d, i + current * page_size, ds_url) for i, d in enumerate(docs)
        ]
        _display_table(columns=_DOC_COLS, rows=rows)
        action = _paginate_select(current=current, last=last, has_same=True)
        action = await action.ask_async()
        match action:
            case _Pagination.NEXT:
                current += 1
            case _Pagination.PREVIOUS:
                current -= 1
            case _Pagination.DONE:
                break
            case _:
                raise ValueError(f"invalid action {action}")


def _paginate_select(current: int, last: int, *, has_same: bool) -> Question:
    actions = []
    default = _Pagination.DONE
    if current != last:
        actions.append(_NEXT_PAGE)
        default = _Pagination.NEXT
    if current != 0:
        actions.append(_PREVIOUS_PAGE)
    if has_same:
        actions.append(_SAME_PAGE)
    actions.append(_DONE)
    return _select("", actions, default=default)


async def _select_candidates(
    candidates: list[dict], ds_url: str, page_size: int = 20
) -> list[dict]:
    n_candidates = len(candidates)
    last = n_candidates // page_size
    current = 0
    selected = defaultdict(set)
    while True:
        page_selection = selected[current]
        start = current * page_size
        end = min(n_candidates, start + page_size)
        docs = [candidates[i] for i in range(start, end)]
        rows = [
            _doc_to_row(d, i + current * page_size, ds_url) for i, d in enumerate(docs)
        ]
        selected_rows = await _select_rows(
            "Select docs", columns=_DOC_COLS, rows=rows, selected=page_selection
        )
        selected[current] = selected_rows
        action = _paginate_select(current=current, last=last, has_same=True)
        action = await action.ask_async()
        match action:
            case _Pagination.NEXT:
                current += 1
            case _Pagination.PREVIOUS:
                current -= 1
            case _Pagination.SAME:
                pass
            case _Pagination.DONE:
                break
    new_candidates = [
        candidates[p * page_size + s] for p, p_sel in selected.items() for s in p_sel
    ]
    return new_candidates


def _normalize_content(content: str, n_lines: int) -> str:
    normalized = content.replace("\r\n", "\n").replace("\n\n", "\n")
    normalized = (line.strip() for line in normalized.split("\n"))
    normalized = [line for line in normalized if line]
    normalized = "\n".join(normalized[:n_lines])
    return normalized


def _doc_to_row(
    doc: dict[str, Any], rank: int, ds_url: str, n_lines: int = 5
) -> tuple[str, str, str, str]:
    rank = str(rank + 1)
    doc_id = doc[ID_]
    root_id = doc[SOURCE].get(DOC_ROOT_ID, doc_id)
    project = doc[INDEX_]
    content_type = doc[SOURCE][DOC_CONTENT_TYPE]
    content = doc[SOURCE][DOC_CONTENT]
    content = _normalize_content(content, n_lines=n_lines)
    url = _make_url(base_url=ds_url, doc_id=doc_id, root_id=root_id, project=project)
    doc = f"[link={url}]{doc_id}[/link]"
    return rank, doc, content_type, content


_TARGET_TEXT_PROMPT = questionary.text("Enter target text: ")


async def _prompt_target_text() -> str:
    text = await _TARGET_TEXT_PROMPT.ask_async()
    return text


_KEYWORD_PROMPT = questionary.text("Enter target text: ")


async def _prompt_term() -> str:
    term = await _KEYWORD_PROMPT.ask_async()
    return term


_SIMILARITIES = [
    questionary.Choice("text", value=SimilarityType.TERM_FREQUENCY),
    questionary.Choice("embedding vector", value=SimilarityType.EMBEDDING),
    questionary.Choice("hybrid (text + embedding vector)", value=SimilarityType.HYBRID),
]

_SIM_TYPE_SELECT = _select(
    "Choose a similarity type:", _SIMILARITIES, default=SimilarityType.TERM_FREQUENCY
)


async def _prompt_similarity_type() -> tuple[SimilarityType, float, float]:
    sim_type = await _SIM_TYPE_SELECT.ask_async()
    text_boost, embedding_boost = 1.0, 1.0
    match sim_type:
        case SimilarityType.TERM_FREQUENCY:
            text_boost = await _prompt_boost("Boost")
        case SimilarityType.EMBEDDING:
            embedding_boost = await _prompt_boost("Boost")
        case SimilarityType.HYBRID:
            embedding_boost = await _prompt_boost("Embedding boost")
            text_boost = await _prompt_boost("Text boost")
    return sim_type, text_boost, embedding_boost


async def _prompt_add_new(item: str) -> bool:
    add = _select(f"Add another {item} ?", ["yes", "no"])
    add = await add.ask_async()
    return add == "yes"


async def _prompt_boost(boost_name: str) -> float:
    text = questionary.text(
        f"{boost_name}:", validate=_validate_float, default=str(1.0)
    )
    sim_type = await text.ask_async()
    return float(sim_type)


def _validate_float(value: str) -> Any:
    try:
        float(value)
    except TypeError:
        return f"{value} is not a valid float"
    return True


def _display_table(columns: Sequence[Column], rows: Sequence[tuple[str, ...]]) -> None:
    width = shutil.get_terminal_size().columns - 2  # room for questionary's pointer
    table = Table(*columns, box=box.HEAVY_HEAD, show_lines=True)
    for row in rows:
        table.add_row(*row)
    console = Console(
        file=sys.stderr, force_terminal=True, color_system="truecolor", width=width
    )
    with console:
        console.print(table)


_TABLE_STYLE = questionary.Style(
    [("highlighted", "reverse"), ("selected", "noreverse")]
)


async def _select_rows(
    select_msg: str,
    *,
    columns: Sequence[Column],
    rows: Sequence[tuple[str]],
    selected: set[int],
) -> list[int]:
    width = shutil.get_terminal_size().columns - 2  # room for questionary's pointer
    table = Table(*columns, box=box.HEAVY_HEAD, show_lines=True)
    for row in rows:
        table.add_row(*row)

    buf = io.StringIO()
    console = Console(
        file=buf, force_terminal=True, color_system="truecolor", width=width
    )
    with console:
        console.print(table)
        lines = buf.getvalue().rstrip("\n").splitlines()

    n_rows = len(rows)
    header_lines, row_lines = lines[: len(lines) - n_rows], lines[len(lines) - n_rows :]
    choices = [questionary.Separator("  " + hl) for hl in header_lines]
    choices += [
        questionary.Choice(
            title=to_formatted_text(ANSI(line)),  # keeps rich's colors
            value=i,
            checked=i in selected,
        )
        for i, line in enumerate(row_lines)
    ]
    selection = questionary.checkbox(select_msg, choices=choices, style=_TABLE_STYLE)
    selection = await selection.ask_async()
    return selection


_EMBEDDINGS_SOURCE = [CHUNK_DOC_ID, CHUNK_EMBEDDING, ID_]


async def _update_doc_vectors(
    client: ESClient, doc_vectors: dict[str, list[Vector]], search: SimilarDocSearch
) -> dict[str, list[Vector]]:
    doc_vectors = defaultdict(list, doc_vectors)
    to_vectorize = {
        like.doc_id
        for like in search.like
        if isinstance(like, LikeDoc) and like.doc_id not in doc_vectors
    }
    to_vectorize = list(to_vectorize)
    # TODO: ideally we should use routing...
    # TODO: poll concurrently
    query = Terms(CHUNK_DOC_ID, to_vectorize)
    query = {QUERY: query.to_dict()}
    # We don't use the pit here since the pit was open on both indexes
    async for page in client.poll_search_pages(
        query, source=_EMBEDDINGS_SOURCE, sort=CHUNK_DOC_ID
    ):
        for chunk in page[HITS][HITS]:
            chunk = chunk[SOURCE]  # noqa: PLW2901
            doc_vectors[chunk[CHUNK_DOC_ID]].append(chunk[CHUNK_EMBEDDING])
    return doc_vectors


async def _update_text_vectors(
    embedder: Embedder,
    chunker: BaseChunker,
    text_vectors: dict,
    search: SimilarDocSearch,
    dim: int,
) -> dict:
    text_vectors = dict(text_vectors)
    to_vectorize = {
        like.text
        for like in search.like
        if isinstance(like, LikeText) and like.text not in text_vectors
    }
    to_vectorize = list(to_vectorize)
    chunks = []
    for text in to_vectorize:
        chunk = list(chunk_text(text, chunker, sep_pattern=None))
        if len(chunk) != 1:
            raise ValueError("expected single chunk")
        chunks.append(chunk[0])
    texts = [chunker.contextualize(chunk) for chunk in chunks]
    embeddings = await embedder.embed(texts, dim)
    for text, embedding in zip(to_vectorize, embeddings, strict=True):
        text_vectors[text] = embedding
    return text_vectors


# TODO: move to datashare_python
def _make_url(*, base_url: str, doc_id: str, root_id: str | None, project: str) -> str:
    base_url = base_url.removesuffix("/")
    if root_id is None:
        root_id = doc_id
    return f"{base_url}/#/ds/{project}/{doc_id}/{root_id}?tab=viewer"
