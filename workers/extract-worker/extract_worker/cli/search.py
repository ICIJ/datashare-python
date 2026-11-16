import json
import logging
import shutil
import warnings
from collections.abc import Sequence
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import questionary
import typer
from datashare_python.objects import Document
from icij_common.pydantic_utils import safe_copy
from prompt_toolkit.formatted_text import FormattedText
from pydantic import ValidationError
from questionary import Question
from questionary import Style as QuestionaryStyle
from rich import box
from rich.color import Color, ColorType
from rich.console import Console
from rich.segment import Segment
from rich.style import Style as RichStyle
from rich.table import Column, Table
from rich.text import Text
from wcwidth import wcswidth

from ..config import EmbeddingWorkerConfig, ESClientConfig, ESClientSetting
from ..constants import (
    CHUNK_TEXT,
    DEFAULT_EMBEDDING_DIM,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_EMBEDDING_MODEL_REVISION,
)
from ..embeddings import HFEmbedder, make_chunker
from ..search import (
    ESRetriever,
    ESRetrieverConfig,
    Like,
    LikeDoc,
    LikeText,
    Retriever,
    SimilarDocSearch,
    SimilarityType,
    Unlike,
    UnlikeDoc,
    UnlikeText,
    WithoutTerms,
    WithTerms,
)
from .utils import AsyncTyper

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
    project: Annotated[str, typer.Argument(help=_SIMILAR_DOCS_PROJECT_HELP)],  # noqa: F821
    search: Annotated[str | None, typer.Option(help=_SIMILAR_DOCS_SEARCH_HELP)] = None,
    model: Annotated[
        str, typer.Option(help=_SIMILAR_DOCS_MODEL_HELP)
    ] = DEFAULT_EMBEDDING_MODEL,
    model_revision: Annotated[
        str | None, typer.Option(help=_SIMILAR_DOCS_MODEL_REVISION_HELP)
    ] = DEFAULT_EMBEDDING_MODEL_REVISION,
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
    embedding_config: Annotated[
        str | None,
        typer.Option("-c", "--embedding-config", help=_SIMILAR_DOCS_EMBEDDER_CONFIG),
    ] = None,
    n: Annotated[
        int,
        typer.Option("-n", help=_SIMILAR_DOCS_HELP),
    ] = 100,
) -> None:
    import transformers  # noqa: PLC0415

    transformers.logging.set_verbosity_error()
    transformers.logging.disable_progress_bar()
    warnings.simplefilter("ignore", (FutureWarning, UserWarning))

    if search is not None:
        search_path = Path(search)
        if search_path.exists():
            search = search_path.read_text()
        search = SimilarDocSearch.model_validate_json(search)
    else:
        search = SimilarDocSearch()
    if embedding_config is None:
        embedding_config = EmbeddingWorkerConfig()
    if elasticsearch is None:
        elasticsearch = ESClientSetting()
    else:
        try:
            elasticsearch = ESClientConfig.model_validate_json(elasticsearch)
        except ValidationError:
            elasticsearch = ESClientSetting(address=elasticsearch)
    retriever_config = ESRetrieverConfig(
        index=project, chunks_index=embedding_config.index_name(project), dim=dim
    )
    es_client = elasticsearch.to_es_client()
    console = _get_console()
    async with es_client:
        with console.status("Loading vectorizer...", spinner=_SPINNER):
            embedder = HFEmbedder(
                embedding_config, model=model, model_revision=model_revision
            )
            chunker = make_chunker(
                embedding_config,
                model=model,
                model_revision=model_revision,
                merge_peers=False,
            )
        retriever = ESRetriever(es_client, embedder, chunker, retriever_config)
        args = SimilarityArgs(ds_url=ds_url, n_candidates=n)
        async with retriever:
            await _similar_docs_loop(retriever, args, search)


_TEXT_FIELDS = [CHUNK_TEXT]


@dataclass(frozen=True)
class SimilarityArgs:
    ds_url: str
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


class _YesNo(StrEnum):
    YES = "yes"
    NO = "no"

    def __bool__(self):
        return self is _YesNo.YES


def _width(s: str) -> int:
    return max(wcswidth(s), 0)


def _prefix_width(*, multi: bool) -> int:
    return _CHECKBOX_PREFIX if multi else _SELECT_PREFIX


_RICH_CONSOLE = ContextVar("rich_console")


def _get_console() -> Console:
    global _RICH_CONSOLE  # noqa: PLW0602
    if not _RICH_CONSOLE.get(None):
        prefix_width = _prefix_width(multi=True)
        width = shutil.get_terminal_size().columns - prefix_width - 1
        console = Console(force_terminal=True, color_system="truecolor", width=width)
        _RICH_CONSOLE.set(console)
    return _RICH_CONSOLE.get()


_SPINNER = "dots12"
_POINTER = "»"
_SELECTED_GLYPH = "●"
_UNSELECTED_GLYPH = "○"
_INDENT = " " * (len(_POINTER) + 2)
_POINTER_PREFIX = _width(_POINTER) + 2
_GLYPH_PREFIX = max(_width(_SELECTED_GLYPH), _width(_UNSELECTED_GLYPH)) + 1
_SELECT_PREFIX = _POINTER_PREFIX
_CHECKBOX_PREFIX = _POINTER_PREFIX + _GLYPH_PREFIX
_SEPARATOR_PREFIX = _POINTER_PREFIX
_STYLE = QuestionaryStyle(
    [("pointer", "bold"), ("selected", "bold"), ("highlighted", "bold")]
)


async def _similar_docs_loop(
    retriever: Retriever, args: SimilarityArgs, search: SimilarDocSearch
) -> None:
    # TODO: add incremental validation
    candidates = []
    try:
        while True:
            if not search.is_empty:
                console = _get_console()
                with console.status("Searching similar docs...", spinner=_SPINNER):
                    candidates = await retriever.retrieve_similar(
                        search, top_k=args.n_candidates
                    )
                if candidates:
                    await _display_candidates(candidates, args.ds_url)
                else:
                    questionary.print("Couldn't find any similar documents")
            search = await _similar_doc_step(search, candidates, ds_url=args.ds_url)
    finally:
        if search is not None:
            print(json.dumps(search.model_dump(), indent=2, sort_keys=True))


def _text(*args: Any, **kwargs: Any) -> Question:
    return questionary.text(*args, **kwargs, qmark="🔍", style=_STYLE)


def _select(*args: Any, **kwargs: Any) -> Question:
    return questionary.select(
        *args, use_shortcuts=True, **kwargs, qmark="🔍", style=_STYLE, pointer=_POINTER
    )


def _checkbox(*args: Any, **kwargs: Any) -> Question:
    return questionary.checkbox(*args, **kwargs, qmark="🔍", pointer=_POINTER)


async def _similar_doc_step(
    search: SimilarDocSearch, candidates: list[Document], *, ds_url: str
) -> SimilarDocSearch:
    actions = await _prompt_step_action(candidates)
    like, unlike = [], []
    match actions:
        case _StepAction.LIKE:
            like = await _prompt_like(candidates, ds_url=ds_url)
        case _StepAction.UNLIKE:
            unlike = await _prompt_unlike(candidates, ds_url=ds_url)
        case _StepAction.EDIT_SEARCH:
            raise NotImplementedError("to be implemented")
        case _StepAction.VALIDATE:
            search = await _validate_candidates(candidates, search, ds_url=ds_url)
        case _StepAction.DISPLAY_RESULTS:
            await _display_candidates(candidates, ds_url)
            return search
        case _StepAction.END:
            raise typer.Exit()
        case _:
            raise ValueError(f"invalid action: {actions}")
    updated = {"like": search.like + like, "unlike": search.unlike + unlike}
    search = safe_copy(search, update=updated)
    return search


_STEP_ACTION_LIKE = questionary.Choice("find similar docs", value=_StepAction.LIKE)
_STEP_ACTION_UNLIKE = questionary.Choice("exclude docs", value=_StepAction.UNLIKE)
_STEP_ACTION_EDIT = questionary.Choice(
    "edit current search", value=_StepAction.EDIT_SEARCH
)
_STEP_ACTION_VALIDATE = questionary.Choice(
    "validate result", value=_StepAction.VALIDATE
)
_STEP_ACTION_DISPLAY = questionary.Choice(
    "display results", value=_StepAction.DISPLAY_RESULTS
)
_STEP_ACTION_TERMINATE = questionary.Choice("terminate", value=_StepAction.END)
_ALL_STEP_ACTIONS = [
    _STEP_ACTION_LIKE,
    _STEP_ACTION_UNLIKE,
    _STEP_ACTION_VALIDATE,
    _STEP_ACTION_EDIT,
    _STEP_ACTION_DISPLAY,
    _STEP_ACTION_TERMINATE,
]


async def _prompt_step_action(candidates: list[Document]) -> _StepAction:
    if not candidates:
        actions = deepcopy([_STEP_ACTION_LIKE, _STEP_ACTION_TERMINATE])
    else:
        actions = deepcopy(_ALL_STEP_ACTIONS)
    direction = await _select(
        "Select an action:", actions, default=_StepAction.LIKE
    ).ask_async()
    return direction


_LIKE_TYPES = [
    questionary.Choice("similarity with other docs", value=_LikeTypes.DOC),
    questionary.Choice("similarity with some text", value=_LikeTypes.TEXT),
    questionary.Choice("by keywords", value=_LikeTypes.WITH_TERMS),
]


async def _prompt_like(candidates: list[Document], *, ds_url: str) -> list[Like]:
    like_type = await _select(
        "Include similar docs by:", _LIKE_TYPES, default=_LikeTypes.DOC
    ).ask_async()

    match like_type:
        case _LikeTypes.DOC:
            likes = await _prompt_like_docs(candidates, ds_url=ds_url)
        case _LikeTypes.TEXT:
            likes = await _prompt_like_texts()
        case _LikeTypes.WITH_TERMS:
            terms = await _prompt_with_terms("keyword")
            likes = [WithTerms(terms=terms)]
        case _:
            raise ValueError(f"unexpected similarity type {like_type}")
    return likes


_UNLIKE_TYPES = [
    questionary.Choice("similarity other docs", value=_LikeTypes.DOC),
    questionary.Choice("similarity with some text", value=_LikeTypes.TEXT),
    questionary.Choice("by keywords", value=_LikeTypes.WITH_TERMS),
]


async def _prompt_unlike(candidates: list[Document], ds_url: str) -> list[Unlike]:
    like_type = _select("Exclude docs by:", _UNLIKE_TYPES, default=_LikeTypes.DOC)
    like_type = await like_type.ask_async()
    match like_type:
        case _LikeTypes.DOC:
            unlikes = await _prompt_unlike_docs(candidates, ds_url=ds_url)
        case _LikeTypes.TEXT:
            unlikes = await _prompt_unlike_texts()
        case _LikeTypes.WITH_TERMS:
            terms = await _prompt_with_terms("negative keyword")
            unlikes = [WithoutTerms(terms=terms)]
        case _:
            raise ValueError(f"unexpected similarity type {like_type}")
    return unlikes


_BY_ID = questionary.Choice("by ID", value=_DocAction.BY_ID)
_FROM_CANDIDATES = questionary.Choice("from results", value=_DocAction.FROM_CANDIDATES)
_SELECTION_DONE = questionary.Choice("done", value=_DocAction.DONE)


def _doc_actions(candidates: list[Document]) -> list[questionary.Choice]:
    actions = [_BY_ID]
    if candidates:
        actions.append(_FROM_CANDIDATES)
    actions.append(_SELECTION_DONE)
    return deepcopy(actions)


async def _prompt_like_docs(
    candidates: list[Document], *, ds_url: str
) -> list[LikeDoc]:
    doc_ids = []
    default = _DocAction.FROM_CANDIDATES if candidates else _DocAction.BY_ID
    action = _select("Add similar docs:", _doc_actions(candidates), default)
    act = await action.ask_async()
    if act is _DocAction.DONE:
        return []
    doc_ids += await _select_docs(candidates, act, ds_url)
    while (
        act := await _select(
            "Add similar docs:", _doc_actions(candidates), _DocAction.DONE
        ).ask_async()
    ) is not _DocAction.DONE:
        doc_ids += await _select_docs(candidates, act, ds_url)
    similarity, text_weight, embedding_weight = await _prompt_similarity_type()
    likes = []
    for doc_id in doc_ids:
        like = LikeDoc(
            doc_id=doc_id,
            similarity=similarity,
            term_frequency_weight=text_weight,
            embedding_weight=embedding_weight,
        )
        likes.append(like)
    return likes


async def _prompt_unlike_docs(
    candidates: list[Document], ds_url: str
) -> list[UnlikeDoc]:
    doc_ids = []
    default = _DocAction.FROM_CANDIDATES if candidates else _DocAction.BY_ID
    action = _select("Add dissimilar docs:", _doc_actions(candidates), default)
    act = await action.ask_async()
    if act is _DocAction.DONE:
        return []
    doc_ids += await _select_docs(candidates, act, ds_url)
    while (
        act := await _select(
            "Add dissimilar docs:", _doc_actions(candidates), default
        ).ask_async()
    ) is not _DocAction.DONE:
        doc_ids += await _select_docs(candidates, act, ds_url)
    weight = await _prompt_weight("Weight")
    unlikes = []
    for doc_id in doc_ids:
        like = UnlikeDoc(doc_id=doc_id, weight=weight)
        unlikes.append(like)
    return unlikes


_TEXT_ACTIONS = [
    questionary.Choice("add another text", value=_TextAction.ADD),
    questionary.Choice("done", value=_TextAction.DONE),
]
_TEXT_ACTION_SELECT = _select("Action:", _TEXT_ACTIONS, _TextAction.DONE)


async def _prompt_like_texts() -> list[LikeText]:
    texts = []
    texts.append(await _prompt_target_text())
    while (await _TEXT_ACTION_SELECT.ask_async()) is not _TextAction.DONE:
        texts.append(await _prompt_target_text())
    similarity, text_weight, embedding_weight = await _prompt_similarity_type()
    likes = []
    for text in texts:
        like = LikeText(
            text=text,
            similarity=similarity,
            term_frequency_weight=text_weight,
            embedding_weight=embedding_weight,
        )
        likes.append(like)
    return likes


async def _prompt_unlike_texts() -> list[UnlikeText]:
    texts = []
    texts.append(await _prompt_target_text())
    while (await _TEXT_ACTION_SELECT.ask_async()) is not _TextAction.DONE:
        texts.append(await _prompt_target_text())
    weight = await _prompt_weight("Weight")
    likes = []
    for text in texts:
        like = UnlikeText(text=text, weight=weight)
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


_YES_CHOICE = questionary.Choice("Yes", _YesNo.YES)
_NO_CHOICE = questionary.Choice("No", _YesNo.NO)
_EXCLUDE_NON_VALIDATED = _select(
    "Add non validated in exclusion query", [_YES_CHOICE, _NO_CHOICE], default=_YesNo.NO
)


async def _validate_candidates(
    candidates: list[Document], search: SimilarDocSearch, ds_url: str
) -> SimilarDocSearch:
    prompt = "Select valid results"
    validated = await _select_candidates(candidates, ds_url, prompt=prompt)
    validated = {d.id for d in validated}
    exclude = await _EXCLUDE_NON_VALIDATED.ask_async()
    excluded = set()
    if exclude:
        excluded = {d.id for d in candidates if d.id not in validated}
    search = search.with_valid_candidates(validated, excluded=excluded)
    return search


_DOC_ID_PROMPT = _text("Enter a doc ID: ", validate=_validate_doc_id)


async def _select_docs(
    candidates: list[Document], selection_type: _DocAction, ds_url: str
) -> list[str]:
    match selection_type:
        case _DocAction.BY_ID:
            doc_ids = await _DOC_ID_PROMPT.ask_async()
            doc_ids = [doc_ids]
        case _DocAction.FROM_CANDIDATES:
            candidates = await _select_candidates(
                candidates, ds_url, prompt="Select docs"
            )
            doc_ids = [d.id for d in candidates]
        case _:
            raise ValueError(f"unexpected selection type {selection_type}")
    return doc_ids


_NEXT_PAGE = questionary.Choice("next page", value=_Pagination.NEXT)
_PREVIOUS_PAGE = questionary.Choice("previous page", value=_Pagination.PREVIOUS)
_DONE = questionary.Choice("done", value=_Pagination.DONE)
_SAME_PAGE = questionary.Choice("same page", value=_Pagination.SAME)


_DOC_COLS = [
    Column(header="Rank", overflow="ellipsis", ratio=1, no_wrap=True),
    Column(header="Doc ID", overflow="ellipsis", ratio=1),
    Column(header="Content Type", overflow="ellipsis", ratio=1, no_wrap=True),
    Column(header="Content", overflow="ellipsis", ratio=3),
]


async def _display_candidates(
    candidates: list[Document], ds_url: str, page_size: int = 10
) -> None:
    n_candidates = len(candidates)
    last = n_candidates // page_size
    current = 0
    while True:
        start = current * page_size
        end = min(n_candidates, start + page_size)
        docs = [candidates[i] for i in range(start, end)]
        rows = [_doc_to_row(d, start + i, ds_url) for i, d in enumerate(docs)]
        if rows:
            _display_table(columns=deepcopy(_DOC_COLS), rows=rows)
            action = _paginate_select(current=current, last=last, has_same=False)
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
    actions = deepcopy(actions)
    return _select("", actions, default=default)


async def _select_candidates(
    candidates: list[Document], ds_url: str, *, prompt: str
) -> list[Document]:
    selected = set()
    rows = [_doc_to_row(d, i, ds_url) for i, d in enumerate(candidates)]
    selected_rows = await _select_rows(
        prompt, columns=deepcopy(_DOC_COLS), rows=rows, selected=selected
    )
    new_candidates = [candidates[i] for i in sorted(selected_rows)]
    return new_candidates


def _normalize_content(
    content: str, n_lines: int, *, max_line_length: int = 300
) -> str:
    normalized = content.replace("\r\n", "\n").replace("\n\n", "\n")
    normalized = (line.strip() for line in normalized.split("\n"))
    normalized = [line[:max_line_length] for line in normalized if line]
    normalized = "\n".join(normalized[:n_lines])
    return normalized


def _doc_to_row(
    doc: Document, rank: int, ds_url: str, n_lines: int = 5
) -> tuple[str, str, str, str]:
    rank = str(rank + 1)
    doc_id = doc.id
    root_id = doc.root_document
    project = doc.index
    content_type = doc.content_type
    content = doc.content
    content = _normalize_content(content, n_lines=n_lines)
    url = _make_url(base_url=ds_url, doc_id=doc_id, root_id=root_id, project=project)
    doc = f"[link={url}]{doc_id}[/link]"
    return rank, doc, content_type, content


_TARGET_TEXT_PROMPT = _text("Enter target text: ")


async def _prompt_target_text() -> str:
    text = await _TARGET_TEXT_PROMPT.ask_async()
    return text


_KEYWORD_PROMPT = _text("Enter target text: ")


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
    text_weight, embedding_weight = 1.0, 1.0
    match sim_type:
        case SimilarityType.TERM_FREQUENCY:
            text_weight = await _prompt_weight("Weight")
        case SimilarityType.EMBEDDING:
            embedding_weight = await _prompt_weight("Weight")
        case SimilarityType.HYBRID:
            embedding_weight = await _prompt_weight("Embedding weight")
            text_weight = await _prompt_weight("Text weight")
    return sim_type, text_weight, embedding_weight


async def _prompt_add_new(item: str) -> bool:
    add = _select(f"Add another {item} ?", ["yes", "no"])
    add = await add.ask_async()
    return add == "yes"


async def _prompt_weight(weight_name: str) -> float:
    text = _text(f"{weight_name}:", validate=_validate_float, default=str(1.0))
    sim_type = await text.ask_async()
    return float(sim_type)


def _validate_float(value: str) -> Any:
    try:
        float(value)
    except TypeError:
        return f"{value} is not a valid float"
    return True


def _display_table(columns: Sequence[Column], rows: Sequence[tuple[str, ...]]) -> None:
    table = Table(*columns, box=box.HEAVY_HEAD, show_lines=True)
    for row in rows:
        table.add_row(*row)
    console = _get_console()
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
    table = _build_table(columns, rows)
    console = _get_console()
    prefix_width = _prefix_width(multi=True)
    lines = [_rstrip_line(line) for line in console.render_lines(table, pad=False)]
    header, row_groups, separators = _split_rows(lines, len(rows))
    body = _row_choices(row_groups, separators, selected, prefix_width)
    selection = _checkbox(
        select_msg,
        choices=_header_choices(header, prefix_width) + body,
        initial_choice=body[0],
        style=_TABLE_STYLE,
    )
    selection = await selection.ask_async()
    return selection


def _build_table(columns: Sequence[Column], rows: Sequence[Sequence[Any]]) -> Table:
    table = Table(
        *columns, box=box.HEAVY_HEAD, show_edge=False, pad_edge=False, show_lines=True
    )
    for row_i, row in enumerate(rows):
        style = RichStyle(meta={"row": row_i})
        table.add_row(*(str(c) for c in row), style=style)  # noqa: F821
    return table


def _is_sep(line: str, *, sentinel: str) -> bool:
    plain = Text.from_ansi(line).plain.strip()
    return bool(plain) and set(plain) <= {sentinel}


def _row_of(line: list[Segment]) -> int | None:
    for seg in line:
        if seg.style is not None and seg.style.meta and "row" in seg.style.meta:
            return seg.style.meta["row"]
    return None


def _split_rows(
    lines: list[list[Segment]], n_rows: int
) -> tuple[list[list[Segment]], list[list[list[Segment]]], list[list[Segment]]]:
    header = []
    rows = [[] for _ in range(n_rows)]
    separators = []
    seen_row = False
    for line in lines:
        i = _row_of(line)
        if i is not None:
            seen_row = True
            rows[i].append(line)
        elif seen_row:
            separators.append(line)  # between-row line, no row meta
        else:
            header.append(line)
    return header, rows, separators


def _plain(line: list[Segment]) -> str:
    return "".join(seg.text for seg in line)


def _header_choices(
    header: list[list[Segment]], prefix_width: int
) -> list[questionary.Separator]:
    pad = " " * max(prefix_width - _SEPARATOR_PREFIX, 0)
    return [questionary.Separator(pad + _plain(line)) for line in header]


def _open_link(url: str) -> str:
    return f"\x1b]8;id={abs(hash(url))};{url}\x1b\\"


_CLOSE_LINK = "\x1b]8;;\x1b\\"

_ZWE = "[ZeroWidthEscape]"


def _hex(color: Color | None) -> str | None:
    if color is None or color.type == ColorType.DEFAULT:
        return None
    t = color.get_truecolor()
    return f"#{t.red:02x}{t.green:02x}{t.blue:02x}"


def _pt_style(style: QuestionaryStyle | None) -> str:
    if style is None:
        return ""
    parts = [
        name
        for name in ("bold", "italic", "underline", "strike", "reverse")
        if getattr(style, name)
    ]
    if fg := _hex(style.color):
        parts.append(f"fg:{fg}")
    if bg := _hex(style.bgcolor):
        parts.append(f"bg:{bg}")
    return " ".join(parts)


def _line_fragments(line: list[Segment]) -> list[tuple[str, str]]:
    fragments = []
    current = None
    for seg in line:
        url = seg.style.link if seg.style else None
        if url != current:
            if current:
                fragments.append((_ZWE, _CLOSE_LINK))
            if url:
                fragments.append((_ZWE, _open_link(url)))
            current = url
        fragments.append((_pt_style(seg.style), seg.text))
    if current:
        fragments.append((_ZWE, _CLOSE_LINK))
    return fragments


def _row_text(group: list[list[Segment]], prefix_width: int) -> FormattedText:
    pad = " " * prefix_width
    fragments = []
    for n, line in enumerate(group):
        if n:
            fragments.append(("", "\n" + pad))
        fragments.extend(_line_fragments(line))
    return FormattedText(fragments)


def _row_choices(
    row_groups: list[list[str]],
    separators: list[list[Segment]],
    selected: set[int],
    prefix_width: int,
) -> list[questionary.Choice]:
    pad = " " * max(prefix_width - _SEPARATOR_PREFIX, 0)
    choices = []
    for i, group in enumerate(row_groups):
        choices.append(
            questionary.Choice(
                title=_row_text(group, prefix_width),
                value=i,
                checked=i in selected,
            )
        )
        if i < len(separators):
            choices.append(questionary.Separator(pad + _plain(separators[i])))
    return choices


def _rstrip_line(line: list[Segment]) -> list[Segment]:
    out = list(line)
    while len(out) > 1 and not out[-1].text.strip():
        out.pop()
    if out:
        last = out[-1]
        out[-1] = Segment(last.text.rstrip(), last.style, last.control)
    return out


# TODO: move to datashare_python
def _make_url(*, base_url: str, doc_id: str, root_id: str | None, project: str) -> str:
    base_url = base_url.removesuffix("/")
    if root_id is None:
        root_id = doc_id
    return f"{base_url}/#/ds/{project}/{doc_id}/{root_id}?tab=text"
