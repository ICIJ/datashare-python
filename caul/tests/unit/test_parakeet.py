from pathlib import Path

import pytest
import torch
from caul.model_cache import cache_parakeet_models
from caul.tasks import (
    ParakeetInferenceRunner,
    ParakeetPostprocessor,
    ParakeetPreprocessor,
)
from caul.tasks.preprocessing.batcher import MaxDurationBatcher
from caul_core import (
    DEFAULT_SAMPLE_RATE,
    ASRPipeline,
    ASRResult,
    ConstantSizeBatcherConfig,
    MaxDurationBatcherConfig,
    ParakeetInferenceRunnerConfig,
    ParakeetPreprocessorConfig,
    Preprocessor,
    SegmentIndex,
    TorchDevice,
)
from huggingface_hub.constants import HF_HUB_CACHE


def test__parakeet_preprocess_inputs_to_fs(tmpdir: Path) -> None:
    # Given
    output_dir = Path(tmpdir)
    preprocessor = ParakeetPreprocessor()
    audio = [torch.zeros([1])]
    # When
    result = list(preprocessor.preprocess_inputs(audio, output_dir=output_dir))
    # Then
    assert len(result) == 1
    result = result[0]
    save_path = output_dir / result.path
    assert save_path.exists()


def test__parakeet_preprocessor_from_config_uses_configured_batcher() -> None:
    # Given
    config = ParakeetPreprocessorConfig(batcher=ConstantSizeBatcherConfig(batch_size=8))
    preprocessor = Preprocessor.from_config(config)
    audio = [torch.zeros([DEFAULT_SAMPLE_RATE]) for _ in range(20)]
    # When
    batches = list(preprocessor.process(audio))
    # Then
    assert [len(b) for b in batches] == [8, 8, 4]


def test__max_duration_batcher_caps_batch_size() -> None:
    # Given
    preprocessor = ParakeetPreprocessor()
    audio = [torch.zeros([DEFAULT_SAMPLE_RATE]) for _ in range(5)]
    config = MaxDurationBatcherConfig(batch_size=2)
    # When
    batches = list(
        MaxDurationBatcher(preprocessor.preprocess_inputs(audio), config).batch()
    )
    # Then
    assert [len(b) for b in batches] == [2, 2, 1]


def test__parakeet_batching_unbatching() -> None:
    """Test audio segmentation with batching (max length 20 minutes) and unbatching"""
    preprocessor = ParakeetPreprocessor()
    samples_per_min = DEFAULT_SAMPLE_RATE * 60

    audio = [torch.zeros([samples_per_min * i]) for i in [12, 11, 5, 4, 7, 10, 30]]

    result = MaxDurationBatcher(preprocessor.preprocess_inputs(audio)).batch()

    expected = [
        [(SegmentIndex(audio=0, segment=0), 12.0)],
        [
            (SegmentIndex(audio=1, segment=0), 11.0),
            (SegmentIndex(audio=2, segment=0), 5.0),
            (SegmentIndex(audio=3, segment=0), 4.0),
        ],
        [
            (SegmentIndex(audio=4, segment=0), 7.0),
            (SegmentIndex(audio=5, segment=0), 10.0),
        ],
        [(SegmentIndex(audio=6, segment=0), 20.0)],
        [(SegmentIndex(audio=6, segment=1), 10.0)],
    ]
    batches = [
        [(r.metadata.index, r.tensor.shape[-1] / samples_per_min) for r in re]
        for re in result
    ]
    assert batches == expected


def test__parakeet_unbatching_should_raise_for_unordered_inputs() -> None:
    # Given
    postprocessor = ParakeetPostprocessor()
    results = [
        ASRResult(
            index=SegmentIndex(audio=2), transcription=[(1, 2, "two one")], score=2.1
        ),
        ASRResult(
            index=SegmentIndex(audio=0), transcription=[(0, 1, "zero")], score=0.0
        ),
        ASRResult(
            index=SegmentIndex(audio=2), transcription=[(2, 3, "two two")], score=2.2
        ),
        ASRResult(
            index=SegmentIndex(audio=1), transcription=[(0, 1, "one")], score=1.0
        ),
    ]

    # When/Then
    expected_msg = "expected contiguous segments"
    with pytest.raises(ValueError, match=expected_msg):
        list(postprocessor.process(results))


def test__parakeet_unbatching() -> None:
    """Test parakeet unbatching including reassembling segmented tensors"""
    postprocessor = ParakeetPostprocessor()

    results = [
        ASRResult(
            index=SegmentIndex(audio=1), transcription=[(0, 1, "one")], score=1.0
        ),
        ASRResult(
            index=SegmentIndex(audio=0), transcription=[(0, 1, "zero")], score=0.0
        ),
        ASRResult(
            index=SegmentIndex(audio=2, segment=0),
            transcription=[(2, 3, "two two")],
            score=2.2,
        ),
        ASRResult(
            index=SegmentIndex(audio=2, segment=1),
            transcription=[(1, 2, "two one")],
            score=2.1,
        ),
    ]

    postprocessed_result = list(postprocessor.process(results))

    expected = [
        results[0],
        results[1],
        ASRResult(
            index=SegmentIndex(audio=2),
            transcription=[(1, 2, "two one"), (2, 3, "two two")],
            score=2.15,
        ),
    ]
    expected = [r.model_dump() for r in expected]
    postprocessed_result = [r.model_dump() for r in postprocessed_result]
    scores = [r.pop("score") for r in postprocessed_result]
    expected_scores = [expected.pop("score") for expected in expected]
    assert postprocessed_result == expected
    assert scores == pytest.approx(expected_scores, abs=1e-6)


def test__parakeet_device_setting() -> None:
    """Test parakeet device setting"""
    # Given
    pipeline = ASRPipeline.parakeet(TorchDevice.CPU)
    assert pipeline.tasks[1].device == TorchDevice.CPU

    # When
    pipeline.device = TorchDevice.MPS

    # Then
    assert pipeline.device == TorchDevice.MPS
    assert pipeline.tasks[1].device == TorchDevice.MPS


@pytest.mark.no_ci
def test_parakeet_should_cache_to_dir_and_load_from_it() -> None:
    # Given
    # Let's use the local cache to avoid downloading for ages
    cache_dir = HF_HUB_CACHE
    # When
    cache_parakeet_models(cache_dir)
    runner = ParakeetInferenceRunner.from_config(ParakeetInferenceRunnerConfig())
    with runner:
        # Then
        assert isinstance(runner._model, torch.nn.Module)
