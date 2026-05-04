import logging
from collections.abc import Generator, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from icij_common.es import DOC_LANGUAGE, SOURCE

from .constants import CONTENT_LENGTH
from .objects import BatchSentence
from .utils import find_device

if TYPE_CHECKING:
    from argostranslate.package import Package
    from argostranslate.tokenizer import BPETokenizer, SentencePieceTokenizer
    from argostranslate.translate import PackageTranslation
    from ctranslate2 import Translator
    from spacy import Language

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class Sentencizer(Protocol):
    def __call__(self, text: str) -> Iterable[str]: ...


@dataclass(frozen=True)
class TranslationEnsemble:
    tokenizer: "SentencePieceTokenizer | BPETokenizer"
    sentencizer: "Sentencizer"
    translator: "Translator"
    target_prefix: str = ""


def translate_as_list(
    sentence_batch: list[BatchSentence],
    translation_ensemble: TranslationEnsemble,
    beam_size: int,
) -> list[str]:
    sentence_batch = [s.sentence for s in sentence_batch]
    return list(_translate(sentence_batch, translation_ensemble, beam_size))


def _translate(
    sentence_batch: list[str],
    translation_ensemble: "TranslationEnsemble",
    beam_size: int,
) -> Generator[str, None, None]:
    tokenized_sentences = [
        translation_ensemble.tokenizer.encode(sentence) for sentence in sentence_batch
    ]

    target_prefix = None

    if translation_ensemble.target_prefix != "":
        target_prefix = [[translation_ensemble.target_prefix]] * len(
            tokenized_sentences
        )

    for translation_result in translation_ensemble.translator.translate_batch(
        tokenized_sentences,
        target_prefix=target_prefix,
        replace_unknowns=True,
        batch_type="tokens",
        beam_size=beam_size,
        num_hypotheses=1,
        length_penalty=0.2,
        return_scores=True,
    ):
        hypothesis = translation_result.hypotheses[0]
        decoded_translation = translation_ensemble.tokenizer.decode(hypothesis)

        if translation_ensemble.target_prefix != "" and decoded_translation.startswith(
            translation_ensemble.target_prefix
        ):
            # Remove target prefix
            decoded_translation = decoded_translation[
                len(translation_ensemble.target_prefix) :
            ]

        yield decoded_translation


def has_language(doc: dict, language: str) -> bool:
    return doc[SOURCE][DOC_LANGUAGE] == language


def _has_language_or_exceeds_max_len(
    doc: dict, language: str, current_batch_byte_len: int, max_batch_byte_len: int
) -> bool:
    return (
        doc[SOURCE][DOC_LANGUAGE] == language
        or doc[SOURCE][CONTENT_LENGTH] + current_batch_byte_len > max_batch_byte_len
    )


def _get_argos_package(
    source_language_alpha_code: str, target_language_alpha_code: str
) -> "Package | None":
    from argostranslate.package import get_installed_packages  # noqa: PLC0415

    available_packages = get_installed_packages()
    return next(
        filter(
            lambda x: (
                x.from_code == source_language_alpha_code
                and x.to_code == target_language_alpha_code
            ),
            available_packages,
        ),
        None,
    )


def _get_argos_languages(
    *languages_to_find: str,
) -> tuple["Language", ...]:
    from argostranslate.translate import get_installed_languages  # noqa: PLC0415

    if not isinstance(languages_to_find, (list, tuple)):
        languages_to_find = [languages_to_find]

    languages = []
    available_languages = get_installed_languages()

    for language_to_find in languages_to_find:
        language_result = next(
            filter(lambda x: x.code == language_to_find, available_languages), None
        )

        if language_result is None:
            continue

        languages.append(language_result)

    return tuple(languages)


def _get_or_download_argos_languages(
    source_language_alpha_code: str, target_language_alpha_code: str
) -> tuple["Language", ...]:
    from argostranslate.package import (  # noqa: PLC0415
        get_available_packages,
        install_from_path,
        update_package_index,
    )

    package = _get_argos_package(source_language_alpha_code, target_language_alpha_code)

    if package is None:
        logger.info(
            "Package %s -> %s not found locally. Checking index.",
            source_language_alpha_code,
            target_language_alpha_code,
        )
        update_package_index()
        available_packages = get_available_packages()
        package_to_install = next(
            filter(
                lambda x: (
                    x.from_code == source_language_alpha_code
                    and x.to_code == target_language_alpha_code
                ),
                available_packages,
            ),
            None,
        )

        if package_to_install is not None:
            logger.info("Downloading argos package %s", package_to_install)
            install_from_path(package_to_install.download())

            _get_argos_package(source_language_alpha_code, target_language_alpha_code)

    return _get_argos_languages(source_language_alpha_code, target_language_alpha_code)


def get_translation_ensemble(
    source_language_alpha_code: str,
    target_language_alpha_code: str,
    device: str = "cpu",
    inter_threads: int = 1,
    intra_threads: int = 0,
    compute_type: str = "auto",
) -> "TranslationEnsemble | None":
    from argostranslate.translate import CachedTranslation  # noqa: PLC0415

    # Create batches per language
    language_packages = _get_or_download_argos_languages(
        source_language_alpha_code, target_language_alpha_code
    )

    if len(language_packages) < 2:
        logger.exception(
            "Language model for %s and/or %s not available. Skipping translation.",
            source_language_alpha_code,
            target_language_alpha_code,
        )
        return None

    source_language_pkg, target_language_pkg = language_packages
    # This is one of the weirder things about argos; it thinks of a translation
    # from language to another as a functional mapping and so treats it as an object
    argos_translation_package: PackageTranslation | None = (
        source_language_pkg.get_translation(target_language_pkg)
    )

    if argos_translation_package is None:
        logger.exception(
            "No translation model exists from %s to %s. Skipping translation.",
            source_language_alpha_code,
            target_language_alpha_code,
        )
        return None

    # Another clumsy and non-transparent implementation by Argos; underlying is also
    # mistyped for returns (should be PackageTranslation, is marked as ITranslation)
    if isinstance(argos_translation_package, CachedTranslation):
        argos_translation_package: PackageTranslation = (
            argos_translation_package.underlying
        )

    return _get_translation_ensemble_from_argos_package(
        argos_translation_package, device, inter_threads, intra_threads, compute_type
    )


def _get_translation_ensemble_from_argos_package(
    argos_package: "PackageTranslation",
    device: str,
    inter_threads: int,
    intra_threads: int,
    compute_type: str,
) -> "TranslationEnsemble":
    import ctranslate2  # noqa: PLC0415

    model_path = str(argos_package.pkg.package_path / "model")
    device = find_device(device)
    translator = ctranslate2.Translator(
        model_path,
        device=device,
        inter_threads=inter_threads,
        intra_threads=intra_threads,
        compute_type=compute_type,
    )

    return TranslationEnsemble(
        sentencizer=argos_package.sentencizer.split_sentences,
        tokenizer=argos_package.pkg.tokenizer,
        translator=translator,
        target_prefix=argos_package.pkg.target_prefix,
    )
