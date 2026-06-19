import gc
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Self

from datashare_python.objects import DatashareLanguage
from pydantic_extra_types.language_code import LanguageAlpha2

from ..config import TranslationWorkerConfig
from ..objects import Language, TranslationModel
from ..processors import Translator
from ..utils import find_device

if TYPE_CHECKING:
    from argostranslate.package import Package
    from argostranslate.translate import Language as ArgosLanguage
    from argostranslate.translate import PackageTranslation

    from ..objects import ArgosTranslatorConfig


logger = logging.getLogger(__name__)


@Translator.register(TranslationModel.ARGOS)
class ArgosTranslator(Translator):
    def __init__(self, config: "ArgosTranslatorConfig"):
        super().__init__(config)
        self._translator = None
        self._tokenizer = None
        self._target_prefix = None

    @classmethod
    def _from_config(cls, config: "ArgosTranslatorConfig", **extras) -> Self:  # noqa: ARG003
        return cls(config)

    def _load(
        self,
        source: Language,
        *,
        target: Language,
        worker_config: TranslationWorkerConfig,
    ) -> None:
        import ctranslate2  # noqa: PLC0415

        super()._load(source, target=target, worker_config=worker_config)

        if isinstance(source, DatashareLanguage):
            source = LanguageAlpha2(source.alpha2)
        if isinstance(target, DatashareLanguage):
            target = LanguageAlpha2(target.alpha2)
        translation_pkg = _load_translation_package(source=source, target=target)
        model_path = str(translation_pkg.pkg.package_path / "model")
        device = find_device(worker_config.device)
        self._tokenizer = translation_pkg.pkg.tokenizer
        self._target_prefix = translation_pkg.pkg.target_prefix
        self._translator = ctranslate2.Translator(
            model_path,
            device=device,
            inter_threads=worker_config.c2_translate.inter_threads,
            intra_threads=worker_config.c2_translate.intra_threads,
            compute_type=worker_config.c2_translate.compute_type,
        )

    def translate(self, texts: Iterable[str]) -> list[str]:
        tokenized = [self._tokenizer.encode(t) for t in texts]
        target_prefix = None
        if self._target_prefix:
            target_prefix = [[self._target_prefix]] * len(tokenized)

        translation_results = self._translator.translate_batch(
            tokenized,
            target_prefix=target_prefix,
            replace_unknowns=True,
            batch_type="tokens",
            beam_size=self._config.beam_size,
            num_hypotheses=1,
            length_penalty=self._config.length_penalty,
            return_scores=True,
        )
        best_hyps = (res.hypotheses[0] for res in translation_results)
        decoded = (self._tokenizer.decode(hyp) for hyp in best_hyps)
        translated = []
        if self._target_prefix:
            for d in decoded:
                if d.startswith(self._target_prefix):
                    translated.append(d[len(self._target_prefix) :])
                else:
                    translated.append(d)
        else:
            translated = list(decoded)
        return list(translated)

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001
        self._tokenizer = None
        self._target_prefix = None
        del self._translator
        self._translator = None
        gc.collect()


def _get_argos_package(
    source: LanguageAlpha2, *, target: LanguageAlpha2
) -> "Package | None":
    from argostranslate.package import get_installed_packages  # noqa: PLC0415

    for p in get_installed_packages():
        if p.from_code == source and p.to_code == target:
            return p
    return None


def get_argos_languages(
    *languages_to_find: LanguageAlpha2,
) -> tuple["ArgosLanguage", ...]:
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
    source: LanguageAlpha2, *, target: LanguageAlpha2
) -> tuple["ArgosLanguage", ...]:
    from argostranslate.package import (  # noqa: PLC0415
        get_available_packages,
        install_from_path,
        update_package_index,
    )

    package = _get_argos_package(source, target=target)
    if package is None:
        logger.info(
            "package %s -> %s not found locally. Checking index...",
            source,
            target,
        )
        update_package_index()
        available_packages = get_available_packages()
        package_to_install = next(
            filter(
                lambda x: x.from_code == source and x.to_code == target,
                available_packages,
            ),
            None,
        )
        if package_to_install is not None:
            logger.info("downloading argos package %s", package_to_install)
            install_from_path(package_to_install.download())

    return get_argos_languages(source, target)


def _load_translation_package(
    source: LanguageAlpha2, *, target: LanguageAlpha2
) -> "PackageTranslation":
    from argostranslate.translate import CachedTranslation  # noqa: PLC0415

    # TODO: we should pre-download and cache instead
    language_packages = _get_or_download_argos_languages(source, target=target)

    if len(language_packages) < 2:
        msg = f"Language model for {source} and/or {target} not available"
        raise ValueError(msg)

    source_pkg, target_pkg = language_packages
    # This is one of the weirder things about argos; it thinks of a translation
    # from language to another as a functional mapping and so treats it as an object
    argos_translation_package = source_pkg.get_translation(target_pkg)
    if argos_translation_package is None:
        msg = f"No translation model exists from {source} to {target}."
        raise ValueError(msg)
    # Another clumsy and non-transparent implementation by Argos; underlying is also
    # mistyped for returns (should be PackageTranslation, is marked as ITranslation)
    if isinstance(argos_translation_package, CachedTranslation):
        argos_translation_package = argos_translation_package.underlying

    return argos_translation_package
