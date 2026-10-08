import logging
from collections.abc import Callable
from pathlib import Path

from caul_core import (
    FIREREDASR2_MODEL_HUB_PREFIX,
    PARAKEET_MODEL_REF,
    PARAKEET_TRT_MODEL_REF,
    ASRModel,
    FasterWhisperModel,
    FireRedASR2ModelRef,
    FireRedASR2ModelTag,
)

from .utils import cache_hf_model, cache_hf_repo

logger = logging.getLogger(__name__)

_FASTER_WHISPER_ALLOW_PATTERNS = [
    "config.json",
    "preprocessor_config.json",
    "model.bin",
    "tokenizer.json",
    "vocabulary.*",
]


def cache_parakeet_models(cache_dir: Path | None = None) -> None:
    cache_hf_model(
        model_family=ASRModel.PARAKEET,
        models=[PARAKEET_MODEL_REF],
        model_ext=".nemo",
        library_name="nemo",
        cache_dir=cache_dir,
    )


def cache_parakeet_trt_models(cache_dir: Path | None = None) -> None:
    cache_hf_model(
        model_family=ASRModel.PARAKEET_TRT,
        models=[PARAKEET_TRT_MODEL_REF],
        model_ext=".engine",
        cache_dir=cache_dir,
    )


def cache_faster_whisper_models(cache_dir: Path | None = None) -> None:
    from huggingface_hub import (
        get_token,
        snapshot_download,
    )

    for model_id in FasterWhisperModel:
        logger.info("Caching faster whisper model of size %s", model_id)
        kwargs = {
            "allow_patterns": _FASTER_WHISPER_ALLOW_PATTERNS,
            "token": get_token(),
        }
        if cache_dir is not None:
            kwargs["cache_dir"] = str(cache_dir)
        snapshot_download(model_id.to_repo_id, **kwargs)


def cache_fireredasr2_models(cache_dir: Path | None = None) -> None:
    for tag in FireRedASR2ModelTag:
        logger.info("Caching FireredASR tag %s", tag)
        repo_id = (
            f"{FIREREDASR2_MODEL_HUB_PREFIX}{FireRedASR2ModelRef.ASR2}-{tag.upper()}"
        )
        cache_hf_repo(repo_id, cache_dir=cache_dir)


def cache_whisper_trt_models(
    cache_dir: Path | None = None,  # noqa: ARG001 shared _MODEL_CACHE_FNS signature
) -> None:
    # Not available yet
    return None


def cache_preprocessing_models(
    cache_dir: Path | None = None,  # noqa: ARG001 shared _MODEL_CACHE_FNS signature
) -> None:
    # We only segment by silence for now, no models to cache
    return None


_MODEL_CACHE_FNS: dict[ASRModel, Callable[[Path | None], None]] = {
    ASRModel.PARAKEET: cache_parakeet_models,
    ASRModel.PARAKEET_TRT: cache_parakeet_trt_models,
    ASRModel.FASTER_WHISPER: cache_faster_whisper_models,
    ASRModel.FIREREDASR2_AED: cache_fireredasr2_models,
    ASRModel.WHISPER_TRT: cache_whisper_trt_models,
    ASRModel.PREPROCESSING: cache_preprocessing_models,
}


def cache_models(asr_model: ASRModel | None, cache_dir: Path | None = None) -> None:
    if asr_model is None:
        logger.info("Caching all models to %s", cache_dir)
        models = list(_MODEL_CACHE_FNS)
    else:
        logger.info("Caching %s model(s) to %s", asr_model, cache_dir)
        if asr_model not in _MODEL_CACHE_FNS:
            raise ValueError(f"Invalid model {asr_model}")
        models = [asr_model]

    for model in models:
        _MODEL_CACHE_FNS[model](cache_dir)
