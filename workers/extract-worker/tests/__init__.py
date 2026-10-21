import tempfile
from pathlib import Path

from datashare_python.objects import ByteRangesPagination, Pages, ProcessedFile
from datashare_python.utils import write_artifact
from extract_worker.objects import (
    MarkdownExtractArgs,
    StructureArtifact,
    StructureManifestEntry,
)

TEST_PATH = Path(__file__).parent
TEST_RESOURCE_PATH = TEST_PATH / "resources"
DOCS_PATH = TEST_RESOURCE_PATH / "docs"

PAGE_SEP = '\n<div style="page-break-after: always;"></div>\n'

PAGES = [
    "# Offshore leaks\n\nShell companies registered in the islands.\n",
    "## Beneficial owners\n\nThe owners were hidden behind nominees.\n",
]


def write_structure(doc: ProcessedFile, artifacts_root: Path, pages: list[str]) -> None:
    contents = [(p + PAGE_SEP).encode() for p in pages[:-1]] + [pages[-1].encode()]
    byte_ranges, start = [], 0
    for c in contents:
        byte_ranges.append((start, start + len(c)))
        start += len(c)
    with tempfile.TemporaryDirectory() as td:
        md_dir = Path(td) / "doc"
        md_dir.mkdir()
        (md_dir / "doc.md").write_bytes(b"".join(contents))
        pages = Pages(
            total=len(contents),
            pagination=ByteRangesPagination(byte_ranges=byte_ranges),
        )
        args = MarkdownExtractArgs(project=doc.project, docs=None)
        artifact = StructureArtifact(
            project=doc.project,
            doc_id=doc.doc_id,
            artifact=md_dir,
            manifest_entry=StructureManifestEntry.complete(args=args, pages=pages),
        )
        write_artifact(artifacts_root, artifact)
