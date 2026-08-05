from collections.abc import AsyncIterable
from itertools import chain
from pathlib import Path
from typing import Any

from datashare_python.objects import Document, WorkerPaths
from datashare_python.utils import (
    ext_to_mime_types,
    symlink_embedded_document_to_workdir,
)
from icij_common.es import (
    DOC_CONTENT_TYPE,
    DOC_LANGUAGE,
    DOC_METADATA,
    DOC_PATH,
    DOC_ROOT_ID,
    ES_DOCUMENT_TYPE,
    HITS,
    QUERY,
    ESClient,
    ESSort,
    and_query,
    has_id,
    has_type,
)
from passport_service.constants import GOTENBERG_SUPPORTED_EXTS, PDF_EXT
from passport_service.core.preprocessing import PIL_SUPPORTED_EXTENSIONS

from .objects import (
    DocId,
    DocumentSearchQuery,
    PreprocessingBatches,
    ProcessedFile,
)
from .utils import write_batches


async def create_preprocessing_batches_act(  # noqa: PLR0917
    docs: list[DocId] | DocumentSearchQuery | None,
    project: str,
    es_client: ESClient,
    paths: WorkerPaths,
    target_n_pages_per_batch: int,
    output_root: Path,
    *,
    supported_image_exts: set[str] | None = None,
    supported_doc_exts: set[str] | None = None,
) -> PreprocessingBatches:
    if supported_image_exts is None:
        supported_image_exts = set(PIL_SUPPORTED_EXTENSIONS)
    if supported_doc_exts is None:
        supported_doc_exts = GOTENBERG_SUPPORTED_EXTS
    supported_image_exts -= {PDF_EXT}
    supported_doc_exts -= supported_image_exts
    pdf_query = _build_doc_query(docs, {PDF_EXT})
    pdf_docs = _search_docs(pdf_query, es_client, project, sort=_DOC_SORT)
    pdf_batches = [
        b
        async for b in _write_preprocessing_batches(
            pdf_docs,
            paths,
            target_n_pages_per_batch,
            output_root,
            batch_offset=0,
        )
    ]
    im_query = _build_doc_query(docs, restrict_image_formats(supported_image_exts))
    im_docs = _search_docs(im_query, es_client, project, sort=_DOC_SORT)
    im_batches = [
        b
        async for b in _write_preprocessing_batches(
            im_docs,
            paths,
            target_n_pages_per_batch,
            output_root,
            batch_offset=len(pdf_batches),
        )
    ]
    to_pdf_query = _build_doc_query(
        docs, restrict_to_pdf_file_formats(supported_doc_exts)
    )
    to_pdf_docs = _search_docs(to_pdf_query, es_client, project, sort=_DOC_SORT)
    to_pdf_batches = [
        b
        async for b in _write_preprocessing_batches(
            to_pdf_docs,
            paths,
            target_n_pages_per_batch,
            output_root,
            batch_offset=len(pdf_batches) + len(im_batches),
        )
    ]
    return PreprocessingBatches(
        to_pdf=to_pdf_batches, images=im_batches, pdfs=pdf_batches
    )


async def _write_preprocessing_batches(
    docs: AsyncIterable[Document],
    paths: WorkerPaths,
    target_n_pages_per_batch: int,
    output_dir: Path,
    batch_offset: int,
) -> AsyncIterable[Path]:
    docs = (symlink_embedded_document_to_workdir(d, paths) async for d in docs)
    batches = _batch_by_n_pages(docs, target_n_pages_per_batch=target_n_pages_per_batch)
    async for p in write_batches(
        batches, output_dir, batch_offset, prefix="preprocessing_batch_"
    ):
        yield p


def _build_doc_query(
    docs: list[DocId] | DocumentSearchQuery | None, supported_exts: set[str]
) -> dict[str, Any]:
    format_query = _with_supported_exts_query(supported_exts)
    match docs:
        case dict():
            if not docs:
                return {QUERY: format_query}
            return and_query(format_query, docs)
        case None:
            return {QUERY: format_query}
        case list():
            return and_query(format_query, has_id(docs))
        case _:
            raise ValueError(f"unsupported format {type(docs)}")


def _with_supported_exts_query(supported_exts: set[str]) -> dict[str, Any]:
    supported_mimes = sorted(chain(*(ext_to_mime_types(f) for f in supported_exts)))
    format_query = {"terms": {DOC_CONTENT_TYPE: supported_mimes}}
    query = and_query(
        format_query, has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)
    )
    return query[QUERY]


_DOC_SORT = [f"{DOC_CONTENT_TYPE}:asc", "_doc:asc"]
_DOC_CONTENT_SOURCES = [DOC_PATH, DOC_ROOT_ID, DOC_LANGUAGE, DOC_METADATA]


async def _search_docs(
    query: dict[str, Any], es_client: ESClient, project: str, sort: ESSort = None
) -> AsyncIterable[ProcessedFile]:
    async for page in es_client.poll_search_pages(
        index=project,
        body=query,
        sort=sort,
        _source_includes=_DOC_CONTENT_SOURCES,
    ):
        for hit in page[HITS][HITS]:
            yield ProcessedFile.from_doc(Document.from_es(hit))


async def _batch_by_n_pages(
    docs: AsyncIterable[ProcessedFile], target_n_pages_per_batch: int
) -> AsyncIterable[list[ProcessedFile]]:
    current_n_pages = 0
    current_batch = []
    async for d in docs:
        if current_n_pages >= target_n_pages_per_batch:
            yield current_batch
            current_n_pages = 0
            current_batch = []
        current_batch.append(d)
        current_n_pages += d.n_pages
    if current_batch:
        yield current_batch


_UNSUPPORTED_IMAGE_FORMATS = {".bw", ".ftu", ".iim", ".im", ".msp"}


def restrict_image_formats(im_formats: set[str]) -> set[str]:
    # We don't want formats which resovles as octet-streams mime type otherwise
    # it will select a lot of invalid image format, some others just have unknown mimes
    return {f for f in im_formats if f not in _UNSUPPORTED_IMAGE_FORMATS}


_UNSUPPORTED_TO_PDF_FORMATS = {
    ".602",
    ".bib",
    ".cwk",
    ".fopd",
    ".hwp",
    ".key",
    ".met",
    ".mw",
    ".odd",
    ".pwp",
}


def restrict_to_pdf_file_formats(im_formats: set[str]) -> set[str]:
    # We don't want formats which resovles as octet-streams mime type otherwise
    # it will select a lot of invalid doc format, some others just have unknown mimes
    return {f for f in im_formats if f not in _UNSUPPORTED_TO_PDF_FORMATS}
