from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from liteparse.types import LayoutComplexityStats, PageComplexityStats

from passport_worker.liteparse_ import (
    LayoutReason,
    LiteparsePDFPreprocessor,
    LiteparsePDFPreprocessorConfig,
    OCRReason,
    may_have_passport,
)
from passport_worker.preprocessing import PDFPreprocessor
from tests import TEST_RESOURCE_PATH


def test_liteparse_pdf_preprocessor_from_config() -> None:
    # Given
    config = LiteparsePDFPreprocessorConfig()
    # When
    processor = PDFPreprocessor.from_config(config)
    # Then
    assert isinstance(processor, LiteparsePDFPreprocessor)


async def test_should_skip_pages(tmp_path: Path) -> None:
    # Given
    path = TEST_RESOURCE_PATH / "liteparse_test.pdf"
    output_dir = Path(tmp_path)
    inner = AsyncMock()
    pages = [Path("some_pdf_page_as.png")]
    inner.process_pdf.return_value = pages
    config = LiteparsePDFPreprocessorConfig()
    preprocessor = LiteparsePDFPreprocessor(config, inner)

    # When
    res = await preprocessor(path, output_dir=output_dir, force_reprocessing=True)

    # Then
    assert res == pages
    expected_skip = {0}
    inner.process_pdf.assert_called_once_with(
        path, expected_skip, output_dir=output_dir, force_reprocessing=True
    )


@pytest.fixture(scope="session")
def test_complexity() -> PageComplexityStats:
    return PageComplexityStats(
        page_number=0,
        text_length=2,
        text_coverage=0.5,
        has_substantial_images=True,
        image_block_count=1,
        image_coverage=0.5,
        largest_image_coverage=0.8,
        full_page_image=True,
        uncovered_vector_area=0.5,
        is_garbled=True,
        page_area=0.5,
        needs_ocr=True,
        reasons=["scanned"],
        layout=LayoutComplexityStats(
            column_count=1,
            ruled_table_count=1,
            ruled_table_coverage=0.5,
            text_table_run_count=1,
            figure_count=1,
            figure_coverage=0.5,
            is_complex=True,
            reasons=["dense-graphics"],
        ),
    )


@pytest.mark.parametrize(
    ("ocr_reasons", "layout_reasons", "expected"),
    [
        (set(), set(), False),
        ({OCRReason.SCANNED}, set(), True),
        ({OCRReason.EMBEDDED_IMAGES}, set(), False),
        (set(), {LayoutReason.DENSE_GRAPHICS}, True),
        (set(), {LayoutReason.TABLE_LIKELY}, False),
    ],
)
async def test_may_have_passport(
    test_complexity: PageComplexityStats,
    ocr_reasons: set[OCRReason],
    layout_reasons: set[LayoutReason],
    *,
    expected: bool,
) -> None:
    # When
    might_have_passport = may_have_passport(
        test_complexity, ocr_reasons=ocr_reasons, layout_reasons=layout_reasons
    )
    # Then
    assert might_have_passport is expected
