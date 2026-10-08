import logging
from collections.abc import Iterable
from functools import cache
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from caul_core import (
    DEFAULT_SAMPLE_RATE,
    FSProcessedSegment,
    MemoryProcessedSegment,
    TorchDevice,
)

from .constants import DEFAULT_BIT_RATE

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import torch
    from torchcodec.decoders import AudioDecoder


def save_tensor(audio: "torch.Tensor", path: Path) -> None:
    """Filesystem routine for audio tensor; defaults to wav

    :param audio: input tensor
    :return: string file uri
    """
    # TODO: Change paths to run_id + tensor uuid + pagination
    #  Allow for remote paths
    from torchcodec.encoders import AudioEncoder

    # Channel required as first dim
    audio = audio.unsqueeze(0)
    encoder = AudioEncoder(samples=audio, sample_rate=DEFAULT_SAMPLE_RATE)
    encoder.to_file(
        path, sample_rate=DEFAULT_SAMPLE_RATE, num_channels=1, bit_rate=DEFAULT_BIT_RATE
    )


def audio_decoder(
    path: str | Path, sample_rate: int | None = None, *, num_channels: int | None = None
) -> "AudioDecoder":
    """Open an audio file for decoding; None keeps the file's native value

    :param path: audio file path
    :param sample_rate: output sample rate
    :param num_channels: output channels
    :return: audio decoder
    """
    from torchcodec.decoders import (
        AudioDecoder,
    )

    return AudioDecoder(path, sample_rate=sample_rate, num_channels=num_channels)


def load_audio(
    path: str | Path, sample_rate: int = DEFAULT_SAMPLE_RATE, *, num_channels: int = 1
) -> "torch.Tensor":
    """Load an audio file as a tensor, resampled to sample_rate

    :param path: audio file path
    :param sample_rate: output sample rate
    :param num_channels: output channels; mono returns a 1D tensor
    :return: audio tensor
    """
    decoder = audio_decoder(path, sample_rate, num_channels=num_channels)
    # Drop channel dim for mono
    return decoder.get_all_samples().data.squeeze(0)


def device_to_torch(device: TorchDevice) -> "torch.device":
    import torch

    match device:
        case TorchDevice.GPU:
            return torch.device("cuda")
        case TorchDevice.CPU:
            return torch.device("cpu")
        case TorchDevice.MPS:
            return torch.device("mps")
        case _:
            raise ValueError(f"unknown device for torch: {device}")


def fuzzy_match(key: str, candidates: set[str]) -> set[str]:
    if key in candidates:
        return {key}
    fuzzy_matches = {k for k in candidates if key in k or k in key}
    return fuzzy_matches


def to_filesystem(
    input_batch: Iterable[FSProcessedSegment | MemoryProcessedSegment],
    output_dir: Path | None,
) -> Iterable[tuple[FSProcessedSegment | MemoryProcessedSegment, Path]]:
    for inp in input_batch:
        inp_id = inp.metadata.uuid

        match inp:
            case FSProcessedSegment():
                wav_path = inp.path
            case MemoryProcessedSegment():
                if output_dir is None:
                    msg = (
                        f"output_dir was not provided for "
                        f"{MemoryProcessedSegment.__name__} input"
                    )
                    raise ValueError(msg)
                index = inp.metadata.index
                filename = f"{index.audio}-{index.segment}-{inp_id}.wav"
                wav_path = output_dir / filename
                save_tensor(inp.tensor, wav_path)
            case _:
                raise TypeError(f"unexpected segment type: {inp}")
        yield inp, wav_path


def cache_hf_model_file(
    repo_id: str,
    *,
    filename: str,
    library_name: str | None = None,
    library_version: str | None = None,
    cache_dir: Path | None = None,
) -> None:
    from huggingface_hub import (
        get_token,
        hf_hub_download,
    )

    hf_hub_download(
        repo_id=repo_id,
        filename=filename,
        cache_dir=cache_dir,
        library_name=library_name,
        library_version=library_version,
        force_download=False,
        token=get_token(),
    )


def cache_hf_repo(
    repo_id: str,
    *,
    library_name: str | None = None,
    library_version: str | None = None,
    cache_dir: Path | None = None,
) -> None:
    from huggingface_hub import (
        get_token,
        snapshot_download,
    )

    snapshot_download(
        repo_id=repo_id,
        cache_dir=cache_dir,
        library_name=library_name,
        library_version=library_version,
        force_download=False,
        token=get_token(),
    )


def cache_hf_model(
    model_family: str,
    models: Iterable[str],
    model_ext: str,
    library_name: str | None = None,
    cache_dir: Path | None = None,
) -> None:
    from huggingface_hub.constants import (
        HF_HUB_CACHE,
    )

    if cache_dir is not None and str(cache_dir) != HF_HUB_CACHE:
        msg = f"{model_family} models must be loaded from {HF_HUB_CACHE}, "
        raise ValueError(msg)

    for m in models:
        logger.info("caching %s model %s", model_family, m)
        filename = PurePosixPath(m).name + model_ext
        cache_hf_model_file(
            repo_id=m, filename=filename, library_name=library_name, cache_dir=cache_dir
        )


@cache
def load_mel_filters(
    n_mels: int,
    mel_filters_path: Path | str = None,
    device: "str | torch.Device" = "cpu",
) -> "torch.Tensor | None":
    """Load the mel filterbank matrix for projecting an stft into a mel spectrogram

    :param n_mels: number of mel filterbank matrix
    :param mel_filters_dir: directory where mel filterbank matrix is saved
    :param device: cpu or cuda
    """
    import numpy as np
    import torch

    if mel_filters_path is None or n_mels not in {80, 128}:
        return None

    with np.load(mel_filters_path) as f:
        return torch.from_numpy(f[f"mel_{n_mels}"]).to(device)
