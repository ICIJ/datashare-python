from pathlib import Path
from unittest.mock import call, patch

import pytest
import torch
from caul.utils import cache_hf_model, to_filesystem
from caul_core import MemoryProcessedSegment, SegmentMetadata

_HF_HUB_DOWNLOAD_PATH = "huggingface_hub.hf_hub_download"
_GET_TOKEN_PATH = "huggingface_hub.get_token"
_HF_HUB_CACHE_PATH = "huggingface_hub.constants.HF_HUB_CACHE"


class TestCacheHfModel:
    def test__forwards_args_through_cache_hf_model_file_to_hf_hub_download(
        self,
    ) -> None:
        cache_dir = Path("/fake/cache")
        models = ["org/model-a", "org/model-b"]
        token = "fake-token"

        with (
            patch(_HF_HUB_CACHE_PATH, str(cache_dir)),
            patch(_GET_TOKEN_PATH, return_value=token),
            patch(_HF_HUB_DOWNLOAD_PATH) as mock_hf_hub_download,
        ):
            cache_hf_model(
                model_family="parakeet",
                models=models,
                model_ext=".engine",
                library_name="nemo",
                cache_dir=cache_dir,
            )

        assert mock_hf_hub_download.call_args_list == [
            call(
                repo_id="org/model-a",
                filename="model-a.engine",
                cache_dir=cache_dir,
                library_name="nemo",
                library_version=None,
                force_download=False,
                token=token,
            ),
            call(
                repo_id="org/model-b",
                filename="model-b.engine",
                cache_dir=cache_dir,
                library_name="nemo",
                library_version=None,
                force_download=False,
                token=token,
            ),
        ]


def test_to_filesystem_should_raise_for_missing_output_dir() -> None:
    # Given
    pp_out = MemoryProcessedSegment(
        metadata=SegmentMetadata(duration_s=1), tensor=torch.zeros([1])
    )
    batches = [pp_out]

    # When/Then
    expected = "output_dir was not provided for MemoryProcessedSegment input"
    with pytest.raises(ValueError, match=expected):
        list(to_filesystem(batches, output_dir=None))


def test_to_filesystem_should_handle_in_memory_tensor(tmpdir: Path) -> None:
    # Given
    pp_out = MemoryProcessedSegment(
        metadata=SegmentMetadata(duration_s=1), tensor=torch.zeros([1])
    )
    batches = [pp_out]

    # When/Then
    out = list(to_filesystem(batches, output_dir=tmpdir))
    assert isinstance(out, list)
