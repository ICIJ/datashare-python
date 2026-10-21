import json
import os
from collections import defaultdict
from collections.abc import AsyncIterable, Iterable
from io import StringIO
from itertools import batched
from pathlib import Path
from typing import Any, Never

import pandas as pd
from aiofile import async_open
from datashare_python.constants import MANIFEST_JSON
from datashare_python.objects import ArtifactType, DocID
from icij_common.es import DOC_ROOT_ID, HITS, ID_, QUERY, SOURCE, ESClient, has_id

from .objects import ExportFormat, PassportArtifact, Passports


async def export_passports_act(
    root: Path,
    export_format: ExportFormat,
    es_client: ESClient,
    *,
    output_path: Path,
    ds_base_url: str,
) -> None:
    match export_format:
        case ExportFormat.CSV:
            lines = export_as_csv(root, ds_base_url=ds_base_url, es_client=es_client)
        case ExportFormat.JSONL:
            lines = export_as_jsonl(root)
        case _:
            raise ValueError(f"unsupported export format {export_format}")
    async with async_open(output_path, "w") as f:
        async for line in lines:
            await f.write(line + "\n")


async def export_as_jsonl(root: Path) -> AsyncIterable[str]:
    async for _, passports in _walk_passports(root):
        yield passports.model_dump_json()


async def export_as_csv(
    root: Path, ds_base_url: str, es_client: ESClient
) -> AsyncIterable[str]:
    out = StringIO()
    records_by_project = defaultdict(list)
    async for r in _read_csv_passport_records(root):
        records_by_project[r["project"]].append(r)
    recs = []
    for project, records in records_by_project.items():
        root_ids = dict()
        batches = batched((d["doc_id"] for d in records), 1000)
        for doc_ids in batches:
            root_ids |= {
                k: v async for k, v in _fetch_root_ids(es_client, project, doc_ids)
            }
        for r in records:
            doc_id = r["doc_id"]
            r["root_id"] = root_ids[doc_id]
            r["url"] = _make_url(
                base_url=ds_base_url,
                doc_id=doc_id,
                root_id=r["root_id"],
                project=project,
            )
            recs.append(r)
    df = pd.DataFrame.from_records(recs).sort_values(by=["doc_id"])
    df.to_csv(out, index=False)
    out.flush()
    for line in out.getvalue().split("\n"):
        if line.strip():
            yield line


async def _read_csv_passport_records(root: Path) -> AsyncIterable[dict[str, Any]]:
    async for (project, doc_id), passports in _walk_passports(root):
        for doc_page in passports.pages:
            page_i = doc_page.page_number
            for passport_page in doc_page.passports:
                as_record = {"page": page_i, "doc_id": doc_id, "project": project}
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


async def _walk_passports(
    root: Path,
) -> AsyncIterable[tuple[tuple[str, DocID], Passports]]:
    # For efficiency, we first check if there's a passport file then open the manifest
    # to see if it's referenced
    for artifact_root, _, files in os.walk(root, onerror=_raise):
        artifact_root = Path(artifact_root)  # noqa: PLW2901
        for f in files:
            if f != PassportArtifact.filename:
                continue
            manifest_path = artifact_root / MANIFEST_JSON
            async with async_open(manifest_path) as m_f:
                manifest_json = await m_f.read()
            manifest = json.loads(manifest_json)
            if ArtifactType.PASSPORTS.value not in manifest:
                continue
            passports_path = artifact_root / f
            async with async_open(passports_path) as p_f:
                passports_json = await p_f.read()
            passports = Passports.model_validate_json(passports_json)
            doc_id = manifest_path.parent.name
            project = manifest_path.parents[3].name
            yield (project, doc_id), passports


def _raise(e: OSError) -> Never:
    raise e


_DOC_ID_SOURCES = [DOC_ROOT_ID]


async def _fetch_root_ids(
    es_client: ESClient, project: str, batch: Iterable[str]
) -> AsyncIterable[tuple[str, str]]:
    query = {QUERY: has_id(list(batch))}
    # We can't use mget because we would need the root id which is precisely what we're
    # looking for
    async for page in es_client.poll_search_pages(
        index=project, body=query, _source_includes=_DOC_ID_SOURCES
    ):
        for hit in page[HITS][HITS]:
            yield hit[ID_], hit[SOURCE][DOC_ROOT_ID]
