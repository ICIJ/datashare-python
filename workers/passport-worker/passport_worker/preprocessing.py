import asyncio
import logging
from abc import abstractmethod
from concurrent.futures import ProcessPoolExecutor
from functools import partial, wraps
from inspect import iscoroutinefunction
from pathlib import Path
from types import TracebackType
from typing import ClassVar, Protocol, Self, TypeVar

from aiofile import async_open
from datashare_python.objects import ProcessedPage, WorkerPaths
from datashare_python.types_ import AsyncProgressRateHandler, SyncProgressRateHandler
from datashare_python.utils import (
    async_read_jsonl_as,
    read_jsonl_as,
    safe_dir,
    to_raw_async_progress,
    to_raw_sync_progress,
)
from icij_common.registrable import RegistrableConfig, RegistrableFromConfig
from passport_service import GotenbergClient
from passport_service.constants import Colorspace
from passport_service.core import process_image, process_pdf
from passport_service.core.preprocessing import PIL_SUPPORTED_EXTENSIONS
from passport_service.exceptions import UnsupportedDocExtension
from passport_service.utils import run_with_concurrency
from pydantic import Field

from passport_worker.objects import (
    DefaultImagePreprocessorConfig,
    FileProcessingError,
    GotenbergPDFConverterConfig,
    ImagePreprocessorType,
    PDFConverterType,
    ProcessedFile,
)

logger = logging.getLogger(__name__)


R = TypeVar("R")


class _PreprocessingFunction[R](Protocol):
    def __call__(self, doc: ProcessedFile, *args, **kwargs) -> R: ...


class ImagePreprocessor(RegistrableFromConfig):
    @abstractmethod
    def __call__(self, image_path: Path, *, output_dir: Path) -> list[Path]: ...

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

    def __call__(self, image_path: Path, *, output_dir: Path) -> list[Path]:
        return process_image(image_path, output_dir=output_dir)

    @classmethod
    def _from_config(cls, config: DefaultImagePreprocessorConfig, **extras) -> Self:
        return cls(config)


class PDFConverterConfig(RegistrableConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="type")
    type: ClassVar[PDFConverterType]


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
        ext = doc.path.suffix
        converted = await self.convert_doc_to_pdf(doc_bytes, ext)
        return converted

    @classmethod
    def _from_config(cls, config: GotenbergPDFConverterConfig, **extras) -> Self:
        return cls(config)


class PDFPreprocessor(Protocol):
    def __call__(
        self, pdf_path: Path, pdf_bytes: bytes, output_dir: Path
    ) -> list[Path]: ...


def reports_errors(
    f: _PreprocessingFunction[R],
) -> _PreprocessingFunction[R | FileProcessingError]:
    from passport_service.core.preprocessing import REPORTED_ERRORS

    if iscoroutinefunction(f):

        @wraps(f)
        async def wrapper(
            doc: ProcessedFile, *args, **kwargs
        ) -> R | FileProcessingError:
            try:
                return await f(doc, *args, **kwargs)
            except REPORTED_ERRORS as e:
                logger.exception("error while processing doc %s", doc)
                report = FileProcessingError.from_exception(doc, e)
                return report
    else:

        @wraps(f)
        def wrapper(doc: ProcessedFile, *args, **kwargs) -> R | FileProcessingError:
            try:
                return f(doc, *args, **kwargs)
            except REPORTED_ERRORS as e:
                logger.exception("error while processing doc %s", doc)
                report = FileProcessingError.from_exception(doc, e)
                return report

    return wrapper


def preprocess_images_act(
    batch: Path,
    paths: WorkerPaths,
    *,
    output_root: Path,
    image_preprocessor: ImagePreprocessor,
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
    output_root: Path,
    progress: AsyncProgressRateHandler | None = None,
) -> tuple[list[ProcessedFile], list[FileProcessingError]]:
    logger.info("converting documents to PDFs, %s docs at a time", max_concurrency)
    docs = [d async for d in async_read_jsonl_as(batch, ProcessedFile)]
    n_docs = len(docs)
    if progress is not None:
        progress = to_raw_async_progress(progress, max_progress=n_docs)
    aws = (_convert_doc_to_pdf(doc, converter, paths, output_root) for doc in docs)
    res_i = 0
    successes = []
    errors = []
    async for res in run_with_concurrency(aws, max_concurrency):
        if isinstance(res, FileProcessingError):
            errors.append(res)
        else:
            successes.append(res)
        if progress is not None and res_i % 10 == 0:
            await progress(res_i)
        res_i += 1
    logger.info(
        "done converting docs to PDfs: %s success, %s errors",
        len(successes),
        len(errors),
    )
    return successes, errors


async def preprocess_pdfs_act(
    batch: Path,
    paths: WorkerPaths,
    pdf_preprocessor: PDFPreprocessor | None = None,
    *,
    output_root: Path,
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
            doc, pdf_preprocessor, paths, output_root=output_root
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


@reports_errors
def _preprocess_image_doc(
    doc: ProcessedFile,
    image_preprocessor: ImagePreprocessor,
    paths: WorkerPaths,
    *,
    output_root: Path,
) -> list[ProcessedPage]:
    ext = doc.path.suffix
    if ext not in PIL_SUPPORTED_EXTENSIONS:
        logger.info("image extension %s not supported !", ext)
        raise UnsupportedDocExtension(ext, PIL_SUPPORTED_EXTENSIONS)
    output_dir = output_root / safe_dir(doc.id) / doc.id
    output_dir.mkdir(parents=True, exist_ok=True)
    im_paths = image_preprocessor(doc.locate(paths), output_dir=output_dir)
    pages = [
        ProcessedPage(page_number=p_i + 1, **doc.child(p, paths).model_dump())
        for p_i, p in enumerate(im_paths)
    ]
    return pages


@reports_errors
async def _convert_doc_to_pdf(
    doc: ProcessedFile, converter: PDFConverter, paths: WorkerPaths, output_root: Path
) -> ProcessedFile:
    async with async_open(doc.locate(paths), "rb") as f:
        doc_bytes = await f.read()
    pdf_bytes = await converter(doc, doc_bytes)
    pdf_path = output_root / safe_dir(doc.id) / f"{doc.id}.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    async with async_open(pdf_path, "wb") as f:
        await f.write(pdf_bytes)
    processed = doc.child(pdf_path, paths)
    return processed


@reports_errors
async def _preprocess_pdf(
    doc: ProcessedFile,
    pdf_processor: PDFPreprocessor,
    paths: WorkerPaths,
    *,
    output_root: Path,
) -> list[ProcessedPage]:
    pdf_path = doc.locate(paths)
    async with async_open(pdf_path, "rb") as f:
        pdf_bytes = await f.read()
    output_dir = output_root / safe_dir(doc.id) / doc.id
    output_dir.mkdir(parents=True, exist_ok=True)
    pages = await asyncio.to_thread(
        pdf_processor, pdf_path, pdf_bytes, output_dir=output_dir
    )
    pages = [
        ProcessedPage(page_number=p_i + 1, **doc.child(p, paths).model_dump())
        for p_i, p in enumerate(pages)
    ]
    return pages
