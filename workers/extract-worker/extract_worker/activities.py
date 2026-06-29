import contextlib
import logging
import mimetypes
import os
from collections.abc import AsyncIterable
from functools import cache
from itertools import chain
from pathlib import Path
from typing import Any, cast

from datashare_python.dependencies import lifespan_es_client, lifespan_worker_config
from datashare_python.objects import DocArtifact, Document, DocumentLocation
from datashare_python.types_ import AsyncProgressRateHandler
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    activity_workdir,
    read_jsonl,
    to_raw_async_progress,
    write_artifact,
)
from extract_core import (
    InputDoc,
    OutputFormat,
    Pipeline,
    PipelineConfig,
    SupportedExt,
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
    SOURCE,
    ESClient,
    ESSort,
    and_query,
    has_id,
    has_type,
)
from pydantic import TypeAdapter
from temporalio import activity

from .config import ExtractWorkerConfig
from .constants import MARKDOWN_DIRNAME, MARKDOWN_METADATA_KEY
from .mimetypes_ import types_map
from .objects import (
    DocId,
    DocumentSearchQuery,
    ErrorReport,
    MarkdownExtractResponse,
    ProcessedDoc,
    ProcessingReport,
)

logger = logging.getLogger(__name__)

mimetypes.init()
PIPELINE_CONFIG_TA = TypeAdapter(PipelineConfig)


class MarkdownExtract(ActivityWithProgress):
    @activity_defn(name="extract.worker-config")
    async def extract_worker_config(self) -> ExtractWorkerConfig:
        logger.debug("fetching worker config...")
        worker_config = cast(ExtractWorkerConfig, lifespan_worker_config())
        return worker_config

    @activity_defn(name="extract.create-markdown-batches")
    async def create_markdown_extract_batches(
        self,
        project: str,
        docs: list[DocId] | DocumentSearchQuery | None,
        config: PipelineConfig,
    ) -> list[Path]:
        es_client = lifespan_es_client()
        worker_config = cast(ExtractWorkerConfig, lifespan_worker_config())
        workdir = worker_config.workdir
        artifacts_root = worker_config.artifacts_root
        output_dir = activity_workdir(workdir, project)
        output_dir.mkdir(parents=True, exist_ok=True)
        target_n_pages_per_batch = worker_config.markdown.target_n_pages_per_batch
        supported_exts = config.supported_exts()
        logger.debug("creating extraction batches...")
        batch_paths = [
            p.relative_to(workdir)
            async for p in create_markdown_extract_batches_act(
                docs,
                project,
                supported_exts,
                artifacts_root=artifacts_root,
                workdir=workdir,
                output_dir=output_dir,
                target_n_pages_per_batch=target_n_pages_per_batch,
                es_client=es_client,
            )
        ]
        logger.debug("created extraction batches !")
        return batch_paths

    @activity_defn(name="extract.extract-markdown-content")
    async def extract_markdown_content(
        self,
        batch: Path,
        project: str,
        config: PipelineConfig,
        *,
        progress: AsyncProgressRateHandler | None = None,
    ) -> MarkdownExtractResponse:
        # Import pipeline impls to make sure the pipeline registry is populated
        from extract_python import (  # noqa: F401, PLC0415
            DoclingPipeline,
            MarkerPipeline,
            MinerUPipeline,
        )

        pipeline = Pipeline.from_config(config)
        worker_config = cast(ExtractWorkerConfig, lifespan_worker_config())
        workdir = worker_config.workdir
        output_dir = activity_workdir(workdir, project)
        output_dir.mkdir(parents=True, exist_ok=True)
        batch = workdir / batch
        logger.debug("extracting doc content as markdown...")
        res = await extract_markdown_content_act(
            pipeline,
            batch,
            worker_config=worker_config,
            output_dir=output_dir,
            progress=progress,
        )
        logger.debug("extracted doc content as markdown !")
        return res


# Sort documents aiming for consistent processing type in a batch
_DOC_SORT = [f"{DOC_CONTENT_TYPE}:asc", f"{DOC_LANGUAGE}:asc", "_doc:asc"]
_DOC_CONTENT_SOURCES = [DOC_PATH, DOC_ROOT_ID, DOC_LANGUAGE, DOC_METADATA]


async def create_markdown_extract_batches_act(
    docs: list[DocId] | DocumentSearchQuery | None,
    project: str,
    supported_exts: set[SupportedExt],
    *,
    artifacts_root: Path,
    workdir: Path,
    output_dir: Path,
    target_n_pages_per_batch: int,
    es_client: ESClient | None = None,
) -> AsyncIterable[Path]:
    # TODO: supported content types should be args
    query = _build_doc_query(docs, supported_exts)
    docs = (
        _symlink_embedded_processed_doc_to_workdir(d, artifacts_root, workdir=workdir)
        async for d in _search_docs(es_client, project, query, sort=_DOC_SORT)
    )
    batches = _batch_by_n_pages(docs, target_n_pages_per_batch=target_n_pages_per_batch)
    async for p in _write_batches(batches, output_dir):
        yield p


_BatchTypeAdapter = TypeAdapter(list[ProcessedDoc])


async def extract_markdown_content_act(
    pipeline: Pipeline,
    batch: Path,
    *,
    worker_config: ExtractWorkerConfig,
    output_dir: Path,
    progress: AsyncProgressRateHandler | None = None,
) -> MarkdownExtractResponse:
    docs = _BatchTypeAdapter.validate_python(list(read_jsonl(batch)))
    if progress is not None:
        progress = to_raw_async_progress(progress, max_progress=len(docs))
    docs_root = worker_config.docs_root
    artifacts_root = worker_config.artifacts_root
    workdir = worker_config.workdir
    input_docs = (
        InputDoc.from_path(
            d.locate(
                original_root=docs_root, artifacts_root=artifacts_root, workdir=workdir
            )
        )
        for d in docs
    )
    results = pipeline.extract_content(
        input_docs, output_format=OutputFormat.MARKDOWN, output_path=output_dir
    )
    docs = iter(docs)
    n_docs, n_pages, n_successes, n_successes_pages = 0, 0, 0, 0
    errors = []
    async for extract_res in results:
        # Heartbeat explicitely to avoid heartbeat timeout
        with contextlib.suppress(RuntimeError):
            activity.heartbeat()
        doc = next(docs)
        n_docs += 1
        n_pages += doc.n_pages
        if extract_res.errors:
            error = ErrorReport(
                doc=doc, status=extract_res.status, errors=extract_res.errors
            )
            errors.append(error)
        else:
            n_successes += 1
            n_successes_pages += doc.n_pages
            md_path = output_dir / extract_res.output.path
            artifact = DocArtifact(
                project=doc.index,
                doc_id=doc.id,
                artifact=md_path,
                metadata_key=MARKDOWN_METADATA_KEY,
                filename=MARKDOWN_DIRNAME,
            )
            write_artifact(artifacts_root, artifact)
        if progress is not None:
            await progress(n_docs)
    processed = ProcessingReport(n_docs=n_docs, n_pages=n_pages)
    successes = ProcessingReport(n_docs=n_successes, n_pages=n_successes_pages)
    response = MarkdownExtractResponse(
        processed=processed, successes=successes, errors=errors
    )
    return response


def _with_supported_exts_query(supported_exts: set[SupportedExt]) -> dict[str, Any]:
    supported_mimes = sorted(chain(*(ext_to_mime_types(f) for f in supported_exts)))
    format_query = {"terms": {DOC_CONTENT_TYPE: supported_mimes}}
    query = and_query(
        format_query, has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)
    )
    return query[QUERY]


def _build_doc_query(
    docs: list[DocId] | DocumentSearchQuery | None, supported_exts: set[SupportedExt]
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
            raise ValueError(f"unsupported format {docs.__class__.__name__}")


async def _search_docs(
    es_client: ESClient, project: str, query: dict[str, Any], sort: ESSort = None
) -> AsyncIterable[ProcessedDoc]:
    async for page in es_client.poll_search_pages(
        index=project,
        body=query,
        sort=sort,
        _source_includes=_DOC_CONTENT_SOURCES,
    ):
        for hit in page[HITS][HITS]:
            n_pages = None
            meta = hit[SOURCE].get(DOC_METADATA)
            if meta is not None:
                n_pages = meta.get("tika_metadata_xmptpg_npages")
            yield ProcessedDoc.from_fs_doc(
                Document.from_es(hit).to_filesystem(), n_pages=n_pages
            )


async def _batch_by_n_pages(
    docs: AsyncIterable[ProcessedDoc], target_n_pages_per_batch: int
) -> AsyncIterable[list[ProcessedDoc]]:
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


async def _write_batches(
    batches: AsyncIterable[list[ProcessedDoc]], root: Path
) -> AsyncIterable[Path]:
    batch_id = 0
    async for batch in batches:
        batch_path = root / f"{batch_id}.jsonl"
        with batch_path.open("w") as f:
            for fs_doc in batch:
                f.write(f"{fs_doc.model_dump_json()}\n")
        yield batch_path
        batch_id += 1


def _symlink_embedded_processed_doc_to_workdir(
    doc: ProcessedDoc, artifacts_root: Path, *, workdir: Path
) -> ProcessedDoc:
    match doc.location:
        case DocumentLocation.ARTIFACTS:
            symlinks_dir = workdir / doc.index / "symlinks"
            symlinks_dir.mkdir(parents=True, exist_ok=True)
            symlink_path = Path(*doc.path.parts[:-1], doc.id)
            # Replace the "raw" with the doc id
            doc_ext = Path(doc.resource_name).suffix
            symlink_path = symlink_path.relative_to(Path(doc.index))
            symlink_path = symlinks_dir / f"{symlink_path}{doc_ext}"
            symlink_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path = artifacts_root / doc.path
            with contextlib.suppress(FileExistsError):
                os.symlink(artifact_path, symlink_path)
            return ProcessedDoc(
                path=symlink_path.relative_to(workdir),
                id=doc.id,
                location=DocumentLocation.WORKDIR,
                index=doc.index,
                resource_name=doc.resource_name,
                n_pages=doc.n_pages,
            )
        case DocumentLocation.ORIGINAL:
            return doc
        case _:
            raise ValueError(f"unsupported location {doc.location}")


@cache
def ext_to_mime_types(ext: SupportedExt) -> set[str]:
    # All particular cases
    match ext:
        case SupportedExt.NXML | SupportedExt.DCLG | SupportedExt.DCLG_XML:
            return ext_to_mime_types(SupportedExt.XML)
        case SupportedExt.ADOC | SupportedExt.ASCIIDOC:
            return {"text/x-asciidoc"}
        case SupportedExt.QMD | SupportedExt.RMD:
            return ext_to_mime_types(SupportedExt.MD)
        case SupportedExt.XBRL:
            return ext_to_mime_types(SupportedExt.HTLM)
    try:
        return types_map()[ext]
    except KeyError as e:
        raise ValueError(f"unsupported mimetype {ext}") from e


ACTIVITIES = [
    MarkdownExtract.extract_worker_config,
    MarkdownExtract.create_markdown_extract_batches,
    MarkdownExtract.extract_markdown_content,
]
