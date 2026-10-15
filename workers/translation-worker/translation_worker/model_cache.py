import logging
from collections.abc import Callable
from pathlib import Path

from .objects import DEFAULT_HUNYUAN_MODEL_REF, TranslationModel

logger = logging.getLogger(__name__)


def cache_hunyuan_model(
    model_ref: str | None = None, cache_dir: Path | None = None
) -> None:
    """Download a Hunyuan model to the HF hub cache.

    :param model_ref: HF repo of the model, defaults to
        ``DEFAULT_HUNYUAN_MODEL_REF``
    :param cache_dir: HF hub cache directory, defaults to ``HF_HUB_CACHE``
    """
    from huggingface_hub import get_token, snapshot_download  # noqa: PLC0415

    if model_ref is None:
        model_ref = DEFAULT_HUNYUAN_MODEL_REF
    logger.info("caching %s to %s", model_ref, cache_dir)
    snapshot_download(repo_id=model_ref, cache_dir=cache_dir, token=get_token())


_MODEL_CACHE_FNS: dict[TranslationModel, Callable[[str | None, Path | None], None]] = {
    TranslationModel.HUNYUAN: cache_hunyuan_model,
}


def cache_models(
    model: TranslationModel,
    cache_dir: Path | None = None,
    *,
    model_ref: str | None = None,
) -> None:
    """Download a translation model to the HF hub cache so workers can share it.

    :param model: the translation model
    :param cache_dir: HF hub cache directory, defaults to ``HF_HUB_CACHE``
    :param model_ref: the model's HF repo, defaults to the model's default
    :raises ValueError: when the model can't be cached
    """
    if model not in _MODEL_CACHE_FNS:
        msg = (
            f"{model} models can't be cached, cachable models: {list(_MODEL_CACHE_FNS)}"
        )
        raise ValueError(msg)
    _MODEL_CACHE_FNS[model](model_ref, cache_dir)
