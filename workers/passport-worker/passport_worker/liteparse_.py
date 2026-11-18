import asyncio
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Self

from datashare_python.utils import run_with_concurrency
from liteparse.parser import LiteParse
from liteparse.types import PageComplexityStats

from .objects import (
    LayoutReason,
    LiteparsePDFPreprocessorConfig,
    OCRReason,
    PDFPreprocessorType,
)
from .preprocessing import DefaultPDFPreprocessor, PDFPreprocessor

logger = logging.getLogger(__name__)


@PDFPreprocessor.register(PDFPreprocessorType.LITEPARSE)
class LiteparsePDFPreprocessor(PDFPreprocessor[LiteparsePDFPreprocessorConfig]):
    def __init__(
        self,
        config: LiteparsePDFPreprocessorConfig,
        inner: DefaultPDFPreprocessor,
        page_batch_size: int = 8,
        concurrency: int | None = None,
    ):
        super().__init__(config)
        self._inner = inner
        self._page_batch_size = page_batch_size
        self._concurrency = concurrency or os.cpu_count()

    async def __call__(
        self, path: Path, *, output_dir: Path, force_reprocessing: bool
    ) -> list[Path]:
        complexities = await self._compute_complexities(path, force_reprocessing)
        ocr_reasons = self._config.page_filter.accept_ocr_reasons
        layout_reasons = self._config.page_filter.accept_layout_reasons
        skip = set()
        for i, c in enumerate(complexities):
            might_be_passport = may_have_passport(
                c, ocr_reasons=ocr_reasons, layout_reasons=layout_reasons
            )
            if might_be_passport:
                skip.add(i)
        pages = await self._inner.process_pdf(
            path, skip, output_dir=output_dir, force_reprocessing=force_reprocessing
        )
        return pages

    async def _compute_complexities(
        self, path: Path, n_pages: int
    ) -> list["PageComplexityStats"]:
        batches = (
            pages for pages in _chunk_docs(n_pages, chunk_size=self._page_batch_size)
        )
        aws = (_compute_batch_complexities(path, batch) for batch in batches)
        complexities = []
        async for res in run_with_concurrency(aws, self._concurrency):
            complexities.extend(res)
        complexities = sorted(complexities, key=lambda c: c.page_number)
        return complexities

    @classmethod
    def _from_config(cls, config: LiteparsePDFPreprocessorConfig, **extras) -> Self:  # noqa: ARG003
        inner = DefaultPDFPreprocessor(config.preprocessor)
        return cls(config, inner)


def may_have_passport(
    complexity: PageComplexityStats,
    ocr_reasons: set[OCRReason],
    layout_reasons: set[LayoutReason],
) -> bool:
    if any(r in ocr_reasons for r in complexity.reasons):
        return True
    if complexity.layout is not None:
        return any(r in layout_reasons for r in complexity.layout.reasons)
    return False


Range = tuple[int, int]


def _chunk_docs(n_pages: int, *, chunk_size: int) -> Iterator[Range]:
    for i in range(0, n_pages, chunk_size):
        start = i
        end = min(start + n_pages, n_pages)
        rng = (start, end)
        yield rng


async def _compute_batch_complexities(
    path: Path, rng: Range
) -> list[PageComplexityStats]:
    target_pages = _liteparse_range(rng)
    parser = LiteParse(ocr_enabled=False, target_pages=target_pages)
    with parser:
        comps = await asyncio.to_thread(parser.is_complex, path)
    return comps


def _liteparse_range(rng: Range) -> str:
    # page range is a Python range, liteparse indexes from 1 and upper bound
    # is inclusive
    return f"{rng[0] + 1}-{rng[1]}"
