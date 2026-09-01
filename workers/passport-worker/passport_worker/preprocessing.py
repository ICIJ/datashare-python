import asyncio
import logging
from abc import abstractmethod
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self, TypeVar

from aiofile import async_open
from aiohttp import ClientResponseError
from datashare_python.objects import ProcessedPage, WorkerPaths
from datashare_python.types_ import AsyncProgressRateHandler, SyncProgressRateHandler
from datashare_python.utils import (
    async_read_jsonl_as,
    read_jsonl_as,
    safe_dir,
    to_raw_async_progress,
    to_raw_sync_progress,
)
from icij_common.registrable import RegistrableFromConfig
from passport_service import GotenbergClient
from passport_service.constants import Colorspace
from passport_service.core import process_image, process_pdf
from passport_service.exceptions import (
    REPORTED_ERRORS,
    ProcessingTimeout,
    UnsupportedDocExtension,
)
from passport_service.utils import run_with_concurrency

from passport_worker.config import GotenbergPDFConverterConfig, PDFConverterType
from passport_worker.constants import pil_supported_extensions
from passport_worker.objects import (
    DefaultImagePreprocessorConfig,
    FileProcessingError,
    ImagePreprocessorType,
    ProcessedFile,
)
from passport_worker.utils import reports_errors

logger = logging.getLogger(__name__)


R = TypeVar("R")


class ImagePreprocessor(RegistrableFromConfig):
    @abstractmethod
    def __call__(
        self, image_path: Path, *, output_dir: Path, force_reprocessing: bool
    ) -> list[Path]: ...

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ): ...


@ImagePreprocessor.register(ImagePreprocessorType.DEFAULT)
class DefaultImagePreprocessor(ImagePreprocessor):
    def __init__(self, config: DefaultImagePreprocessorConfig | None = None):
        if config is None:
            config = DefaultImagePreprocessorConfig()
        self._config = config

    def __call__(
        self, image_path: Path, *, output_dir: Path, force_reprocessing: bool
    ) -> list[Path]:
        output_dir.mkdir(parents=True, exist_ok=True)
        return process_image(
            image_path, output_dir=output_dir, force_reprocessing=force_reprocessing
        )

    @classmethod
    def _from_config(cls, config: DefaultImagePreprocessorConfig, **extras) -> Self:  # noqa:ARG003
        return cls(config)


class PDFConverter(RegistrableFromConfig):
    max_concurrency: int = 10

    @abstractmethod
    async def __call__(self, doc: ProcessedFile, doc_bytes: bytes) -> bytes: ...


@PDFConverter.register(PDFConverterType.GOTENBERG)
class GotenbergPDFConverter(GotenbergClient, PDFConverter):
    def __init__(self, config: GotenbergPDFConverterConfig):
        kwargs = config.model_dump()
        kwargs["service_url"] = kwargs.pop("gotenberg_url")
        kwargs.pop("type")
        super().__init__(**kwargs)

    async def __call__(self, doc: ProcessedFile, doc_bytes: bytes) -> bytes:
        ext = doc.path.suffix.lower()
        try:
            converted = await self.convert_doc_to_pdf(doc_bytes, ext)
        except ClientResponseError as e:
            if e.status == 429:
                raise ProcessingTimeout(doc.path) from e
            raise
        return converted

    @classmethod
    def _from_config(cls, config: GotenbergPDFConverterConfig, **extras) -> Self:  # noqa:ARG003
        return cls(config)


class PDFPreprocessor(Protocol):
    def __call__(
        self,
        pdf_path: Path,
        pdf_bytes: bytes,
        output_dir: Path,
        *,
        force_reprocessing: bool,
    ) -> list[Path]: ...


def preprocess_images_act(
    batch: Path,
    paths: WorkerPaths,
    *,
    output_root: Path,
    image_preprocessor: ImagePreprocessor,
    force_reprocessing: bool = True,
    executor: ProcessPoolExecutor | None = None,
    chunk_size: int = 1,
    event_loop: asyncio.AbstractEventLoop | None = None,
    progress: SyncProgressRateHandler | None = None,
) -> tuple[list[ProcessedPage], list[FileProcessingError]]:
    if executor is None:
        executor = ProcessPoolExecutor(max_workers=1)
    n_processes = executor._max_workers
    logger.info("preprocessing images with %s worker processes", n_processes)
    docs = list(read_jsonl_as(batch, ProcessedFile))
    n_docs = len(docs)
    chunk_size = 1 if n_docs < n_processes * chunk_size else chunk_size
    process_doc_fn = partial(
        _preprocess_image_doc,
        force_reprocessing=force_reprocessing,
        image_preprocessor=image_preprocessor,
        paths=paths,
        output_root=output_root,
    )
    if progress is not None:
        progress = to_raw_sync_progress(progress, max_progress=n_docs)
    errors = []
    successes = []
    for res_i, res in enumerate(
        executor.map(process_doc_fn, docs, chunksize=chunk_size)
    ):
        if isinstance(res, FileProcessingError):
            errors.append(res)
        else:
            successes.extend(res)
        if progress is not None and res_i % 10 == 0 and event_loop:
            progress(res_i, event_loop)
    logger.info(
        "done preprocessing: %s success, %s errors", len(successes), len(errors)
    )
    return successes, errors


async def convert_to_pdfs_act(
    batch: Path,
    converter: PDFConverter,
    paths: WorkerPaths,
    max_concurrency: int,
    *,
    force_reprocessing: bool,
    output_root: Path,
    progress: AsyncProgressRateHandler | None = None,
) -> tuple[list[ProcessedFile], list[FileProcessingError]]:
    logger.info("converting documents to PDFs, %s docs at a time", max_concurrency)
    docs = [d async for d in async_read_jsonl_as(batch, ProcessedFile)]
    n_docs = len(docs)
    if progress is not None:
        progress = to_raw_async_progress(progress, max_progress=n_docs)
    aws = (
        _convert_doc_to_pdf(
            doc, converter, paths, output_root, force_reprocessing=force_reprocessing
        )
        for doc in docs
    )
    res_i = 0
    successes = []
    errors = []
    progress_modulo = max(n_docs // 5, 1)
    async for res in run_with_concurrency(aws, max_concurrency):
        if isinstance(res, FileProcessingError):
            errors.append(res)
        else:
            successes.append(res)
        if progress is not None and res_i % progress_modulo == 0:
            await progress(res_i)
        res_i += 1
    logger.info(
        "done converting docs to PDFs: %s success, %s errors",
        len(successes),
        len(errors),
    )
    return successes, errors


@reports_errors(errors=REPORTED_ERRORS)
def _preprocess_image_doc(
    doc: ProcessedFile,
    image_preprocessor: ImagePreprocessor,
    paths: WorkerPaths,
    *,
    output_root: Path,
    force_reprocessing: bool,
) -> list[ProcessedPage]:
    ext = doc.path.suffix.lower()
    if ext not in pil_supported_extensions():
        logger.info("image extension %s not supported !", ext)
        raise UnsupportedDocExtension(ext, sorted(pil_supported_extensions()))
    output_dir = output_root / safe_dir(doc.id) / doc.id
    im_paths = image_preprocessor(
        doc.locate(paths), output_dir=output_dir, force_reprocessing=force_reprocessing
    )
    pages = [
        ProcessedPage(page_number=p_i + 1, **doc.child(p, paths).model_dump())
        for p_i, p in enumerate(im_paths)
    ]
    return pages


async def preprocess_pdfs_act(
    batch: Path,
    paths: WorkerPaths,
    pdf_preprocessor: PDFPreprocessor | None = None,
    *,
    output_root: Path,
    force_reprocessing: bool,
    progress: AsyncProgressRateHandler | None = None,
) -> tuple[list[ProcessedPage], list[FileProcessingError]]:
    if pdf_preprocessor is None:
        pdf_preprocessor = partial(process_pdf, colorspace=Colorspace.RGB)
    docs = [d async for d in async_read_jsonl_as(batch, ProcessedFile)]
    n_docs = len(docs)
    if progress is not None:
        progress = to_raw_async_progress(progress, max_progress=n_docs)
    successes = []
    errors = []
    for doc_i, doc in enumerate(docs):
        res = await _preprocess_pdf(
            doc,
            pdf_preprocessor,
            paths,
            force_reprocessing=force_reprocessing,
            output_root=output_root,
        )
        if isinstance(res, FileProcessingError):
            errors.append(res)
        else:
            successes.extend(res)
        if progress is not None and doc_i % 10 == 0:
            await progress(doc_i)
    logger.info(
        "done preprocessing PDFs: %s success, %s errors", len(successes), len(errors)
    )
    return successes, errors


@reports_errors(errors=REPORTED_ERRORS)
async def _convert_doc_to_pdf(
    doc: ProcessedFile,
    converter: PDFConverter,
    paths: WorkerPaths,
    output_root: Path,
    *,
    force_reprocessing: bool,
) -> ProcessedFile:
    pdf_path = output_root / safe_dir(doc.id) / f"{doc.id}.pdf"
    valid_pdf = await is_valid_pdf(pdf_path)
    if force_reprocessing or not valid_pdf:
        async with async_open(doc.locate(paths), "rb") as f:
            doc_bytes = await f.read()
        pdf_bytes = await converter(doc, doc_bytes)
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        async with async_open(pdf_path, "wb") as f:
            await f.write(pdf_bytes)
    processed = doc.child(pdf_path, paths)
    return processed


@reports_errors(errors=REPORTED_ERRORS)
async def _preprocess_pdf(
    doc: ProcessedFile,
    pdf_processor: PDFPreprocessor,
    paths: WorkerPaths,
    *,
    force_reprocessing: bool,
    output_root: Path,
) -> list[ProcessedPage]:
    pdf_path = doc.locate(paths)
    async with async_open(pdf_path, "rb") as f:
        pdf_bytes = await f.read()
    output_dir = output_root / safe_dir(doc.id) / doc.id
    output_dir.mkdir(parents=True, exist_ok=True)
    pages = await asyncio.to_thread(
        pdf_processor,
        pdf_path,
        pdf_bytes,
        output_dir=output_dir,
        force_reprocessing=force_reprocessing,
    )
    pages = [
        ProcessedPage(page_number=p_i + 1, **doc.child(p, paths).model_dump())
        for p_i, p in enumerate(pages)
    ]
    return pages


async def is_valid_pdf(path: Path) -> bool:
    import pymupdf  # noqa: PLC0415

    if not path.exists():
        return False
    async with async_open(path, "rb") as f:
        pdf_bytes = await f.read()

    doc = None
    try:
        doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
        return doc.is_pdf
    except Exception:  # noqa: BLE001
        return False
    finally:
        if doc is not None:
            doc.close()
