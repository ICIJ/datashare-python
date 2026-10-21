import logging
from collections import defaultdict
from collections.abc import AsyncIterable, Iterable
from contextlib import suppress
from io import StringIO
from pathlib import Path
from typing import Any, Never

import pandas as pd
from aiofile import async_open
from datashare_python.exceptions import ArtifactNotFound
from datashare_python.objects import Document, WorkerPaths
from datashare_python.utils import async_read_artifact
from temporalio import activity

from .objects import ExportFormat, PassportArtifact, Passports

logger = logging.getLogger(__name__)


async def export_passports_act(
    docs: AsyncIterable[Document],
    export_format: ExportFormat,
    paths: WorkerPaths,
    *,
    output_path: Path,
    ds_base_url: str,
) -> None:
    match export_format:
        case ExportFormat.CSV:
            lines = export_as_csv(docs, ds_base_url=ds_base_url, paths=paths)
        case ExportFormat.JSONL:
            lines = export_as_jsonl(docs, paths)
        case _:
            raise ValueError(f"unsupported export format {export_format}")
    l_count = 0
    async with async_open(output_path, "w") as f:
        async for line in lines:
            await f.write(line + "\n")
            l_count += 1
        if l_count % 1000 == 0:
            with suppress(RuntimeError):
                activity.heartbeat()


async def export_as_jsonl(
    docs: AsyncIterable[Document], paths: WorkerPaths
) -> AsyncIterable[str]:
    async for _, p in _read_passport_artifacts(paths, docs):
        yield p


async def export_as_csv(
    docs: AsyncIterable[Document], ds_base_url: str, paths: WorkerPaths
) -> AsyncIterable[str]:
    out = StringIO()
    records_by_project = defaultdict(list)
    async for doc, passports in _read_passport_artifacts(paths, docs):
        passports = Passports.model_validate_json(passports)
        for r in _record_from_passports(doc, passports):
            records_by_project[r["project"]].append((doc, r))
    recs = []
    for project, records in records_by_project.items():
        for doc, r in records:
            r["root_id"] = doc.root_document
            r["url"] = _make_url(
                base_url=ds_base_url,
                doc_id=doc.id,
                root_id=doc.root_document,
                project=project,
            )
            recs.append(r)
    df = pd.DataFrame.from_records(recs).sort_values(by=["doc_id"])
    df.to_csv(out, index=False)
    out.flush()
    for line in out.getvalue().split("\n"):
        if line.strip():
            yield line


async def _read_passport_artifacts(
    paths: WorkerPaths,
    docs: AsyncIterable[Document],
) -> AsyncIterable[Document, bytes]:
    n_docs = 0
    async for doc in docs:
        try:
            passports = await async_read_artifact(
                paths, doc.id, PassportArtifact, project=doc.project
            )
        except ArtifactNotFound:
            continue
        yield doc, passports
        n_docs += 1
        if n_docs % 100 == 0:
            logger.info(f"processed {n_docs} docs")


def _record_from_passports(
    doc: Document, passports: Passports
) -> Iterable[dict[str, Any]]:
    for doc_page in passports.pages:
        page_i = doc_page.page_number
        for passport_page in doc_page.passports:
            as_record = {"page": page_i, "doc_id": doc.id, "project": doc.project}
            as_record["confidence"] = passport_page.confidence
            mrz = passport_page.mrz
            if mrz is not None:
                as_record["mzr_country"] = mrz.country
                for k, v in mrz.metadata.items():
                    as_record[f"mrz_{k}"] = v
            yield as_record


def _make_url(*, base_url: str, doc_id: str, root_id: str | None, project: str) -> str:
    base_url = base_url.removesuffix("/")
    if root_id is None:
        root_id = doc_id
    return f"{base_url}/#/ds/{project}/{doc_id}/{root_id}?tab=viewer"


def _raise(e: OSError) -> Never:
    raise e
