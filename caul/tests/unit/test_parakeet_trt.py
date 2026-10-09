from unittest.mock import MagicMock, mock_open, patch

import pytest
import torch
from caul.exception import TrtEngineLoadError
from caul.tasks.inference.parakeet_trt import ParakeetTrtInferenceRunner
from caul_core import PARAKEET_MODEL_REF
from omegaconf import OmegaConf

_ENGINE_PATH = "/fake/encoder.trt"
_INFERENCE_HANDLER_PATH = "caul.tasks.inference.parakeet_trt.TrtInferenceHandler"
_PROCESS_TIMESTAMPS_PATH = (
    "nemo.collections.asr.parts.utils.timestamp_utils.process_timestamp_outputs"
)
_BATCH_SIZE = 2
_SIGNAL_LEN = 16000
_AUDIO_INPUT = torch.zeros(_BATCH_SIZE, _SIGNAL_LEN)
_ENC_OUT = torch.zeros((_BATCH_SIZE, 50, 256), dtype=torch.float32)
_ENC_OUT_LEN = torch.full((_BATCH_SIZE,), 50, dtype=torch.int32)


def _mock_inference_runner() -> ParakeetTrtInferenceRunner:
    runner = ParakeetTrtInferenceRunner(PARAKEET_MODEL_REF, _ENGINE_PATH)
    runner._encoder = MagicMock()
    runner._decoder = MagicMock()
    return runner


def _mock_trt_handler(enc_out: torch.Tensor, enc_len: torch.Tensor) -> MagicMock:
    instance = MagicMock()
    instance.infer.return_value = (enc_out, enc_len)
    instance.__enter__ = MagicMock(return_value=instance)
    instance.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=instance)


class TestParakeetTrtInferenceRunner:
    def test__builds_length_tensor_from_audio_shape(self) -> None:
        mock_inference_runner = _mock_inference_runner()
        mock_trt_handler = _mock_trt_handler(_ENC_OUT, _ENC_OUT_LEN)

        with (
            patch(_INFERENCE_HANDLER_PATH, mock_trt_handler),
            patch(_PROCESS_TIMESTAMPS_PATH),
        ):
            mock_inference_runner._transcribe(
                _AUDIO_INPUT, trt_device=torch.device("cpu")
            )

        call_args = mock_trt_handler.return_value.infer.call_args[0][0]
        input_signal_length = call_args["input_signal_length"]
        assert input_signal_length.shape == (_BATCH_SIZE,)
        assert input_signal_length.tolist() == [_SIGNAL_LEN] * _BATCH_SIZE

    def test__encoder_outputs_forwarded_to_decoder(self) -> None:
        mock_inference_runner = _mock_inference_runner()
        mock_trt_handler = _mock_trt_handler(_ENC_OUT, _ENC_OUT_LEN)

        with (
            patch(_INFERENCE_HANDLER_PATH, mock_trt_handler),
            patch(_PROCESS_TIMESTAMPS_PATH),
        ):
            mock_inference_runner._transcribe(
                _AUDIO_INPUT, trt_device=torch.device("cpu")
            )

        enc_out_arg, enc_len_arg = (
            mock_inference_runner._decoder.decoding.rnnt_decoder_predictions_tensor.call_args[
                0
            ]
        )
        assert torch.equal(enc_out_arg, _ENC_OUT)
        assert torch.equal(enc_len_arg, _ENC_OUT_LEN)

    def test__returns_decoder_predictions(self) -> None:
        mock_inference_runner = _mock_inference_runner()
        mock_trt_handler = _mock_trt_handler(_ENC_OUT, _ENC_OUT_LEN)
        expected = [MagicMock(), MagicMock()]
        decoding = mock_inference_runner._decoder.decoding
        decoding.rnnt_decoder_predictions_tensor.return_value = expected

        with (
            patch(_INFERENCE_HANDLER_PATH, mock_trt_handler),
            patch(_PROCESS_TIMESTAMPS_PATH, side_effect=lambda hyps, *_: hyps),
        ):
            result = mock_inference_runner._transcribe(
                _AUDIO_INPUT, trt_device=torch.device("cpu")
            )

        assert result is expected

    def test__converts_decoder_timestamps_to_seconds(self) -> None:
        mock_inference_runner = _mock_inference_runner()
        mock_trt_handler = _mock_trt_handler(_ENC_OUT, _ENC_OUT_LEN)
        decoder = mock_inference_runner._decoder
        decoder.encoder.subsampling_factor = 8
        decoder.cfg.preprocessor.window_stride = 0.01
        hypotheses = [MagicMock(), MagicMock()]
        decoder.decoding.rnnt_decoder_predictions_tensor.return_value = hypotheses
        processed = [MagicMock(), MagicMock()]

        with (
            patch(_INFERENCE_HANDLER_PATH, mock_trt_handler),
            patch(_PROCESS_TIMESTAMPS_PATH, return_value=processed) as mock_process,
        ):
            result = mock_inference_runner._transcribe(
                _AUDIO_INPUT, trt_device=torch.device("cpu")
            )

        mock_process.assert_called_once_with(hypotheses, 8, 0.01)
        assert result is processed

    def test__requests_hypotheses_with_timestamps_from_decoder(self) -> None:
        mock_inference_runner = _mock_inference_runner()
        mock_trt_handler = _mock_trt_handler(_ENC_OUT, _ENC_OUT_LEN)

        with (
            patch(_INFERENCE_HANDLER_PATH, mock_trt_handler),
            patch(_PROCESS_TIMESTAMPS_PATH),
        ):
            mock_inference_runner._transcribe(
                _AUDIO_INPUT, trt_device=torch.device("cpu")
            )

        decoding = mock_inference_runner._decoder.decoding
        call_kwargs = decoding.rnnt_decoder_predictions_tensor.call_args.kwargs
        assert call_kwargs.get("return_hypotheses") is True

    def test__builds_length_tensor_from_original_audio_not_padded_audio(self) -> None:
        mock_inference_runner = _mock_inference_runner()
        mock_trt_handler = _mock_trt_handler(_ENC_OUT, _ENC_OUT_LEN)
        short_audio = torch.zeros(_SIGNAL_LEN // 2)
        long_audio = torch.zeros(_SIGNAL_LEN)

        with (
            patch(_INFERENCE_HANDLER_PATH, mock_trt_handler),
            patch(_PROCESS_TIMESTAMPS_PATH),
        ):
            mock_inference_runner._transcribe(
                [short_audio, long_audio], trt_device=torch.device("cpu")
            )

        call_args = mock_trt_handler.return_value.infer.call_args[0][0]
        input_signal = call_args["input_signal"]
        input_signal_length = call_args["input_signal_length"]

        assert input_signal.shape == (2, _SIGNAL_LEN)
        assert input_signal_length.shape == (2,)
        assert input_signal_length.tolist() == [_SIGNAL_LEN // 2, _SIGNAL_LEN]

    def test__loads_path_inputs_as_tensors(self) -> None:
        mock_inference_runner = _mock_inference_runner()
        mock_trt_handler = _mock_trt_handler(_ENC_OUT, _ENC_OUT_LEN)
        loaded = [torch.zeros(_SIGNAL_LEN), torch.zeros(_SIGNAL_LEN // 2)]

        with (
            patch(_INFERENCE_HANDLER_PATH, mock_trt_handler),
            patch(_PROCESS_TIMESTAMPS_PATH),
            patch(
                "caul.tasks.inference.parakeet_trt.load_audio", side_effect=loaded
            ) as mock_load,
        ):
            mock_inference_runner._transcribe(
                ["/fake/a.wav", "/fake/b.wav"], trt_device=torch.device("cpu")
            )

        assert [c.args[0] for c in mock_load.call_args_list] == [
            "/fake/a.wav",
            "/fake/b.wav",
        ]
        call_args = mock_trt_handler.return_value.infer.call_args[0][0]
        assert call_args["input_signal"].shape == (2, _SIGNAL_LEN)
        assert call_args["input_signal_length"].tolist() == [
            _SIGNAL_LEN,
            _SIGNAL_LEN // 2,
        ]

    def test__raises_when_engine_fails_to_deserialize(self) -> None:
        runner = ParakeetTrtInferenceRunner(PARAKEET_MODEL_REF, _ENGINE_PATH)
        mock_trt = MagicMock(__version__="11.2.1.2")
        mock_trt.Runtime.return_value.deserialize_cuda_engine.return_value = None

        with (
            patch(
                "caul.tasks.inference.parakeet_trt.import_trt", return_value=mock_trt
            ),
            patch("builtins.open", mock_open(read_data=b"engine")),
            pytest.raises(TrtEngineLoadError, match=r"encoder\.trt.*11\.2\.1\.2"),
        ):
            runner.__enter__()

        assert runner._decoder is None

    def test__restores_decoder_with_compute_timestamps(self) -> None:
        runner = ParakeetTrtInferenceRunner(PARAKEET_MODEL_REF, _ENGINE_PATH)
        config = OmegaConf.create({"decoding": {"strategy": "greedy_batch"}})
        OmegaConf.set_struct(config, value=True)
        mock_restore = MagicMock(side_effect=[config, MagicMock()])

        with (
            patch("caul.tasks.inference.parakeet_trt.import_trt"),
            patch("builtins.open", mock_open(read_data=b"engine")),
            patch("nemo.collections.asr.models.ASRModel.restore_from", mock_restore),
        ):
            runner.__enter__()

        config_call, model_call = mock_restore.call_args_list
        assert config_call.kwargs["return_config"] is True
        override = model_call.kwargs["override_config_path"]
        assert override.decoding.compute_timestamps is True

    def test__pads_short_batches_to_engine_minimum(self) -> None:
        mock_inference_runner = _mock_inference_runner()
        mock_trt_handler = _mock_trt_handler(_ENC_OUT, _ENC_OUT_LEN)
        short_audio = [torch.ones(1600), torch.ones(8000)]

        with (
            patch(_INFERENCE_HANDLER_PATH, mock_trt_handler),
            patch(_PROCESS_TIMESTAMPS_PATH),
        ):
            mock_inference_runner._transcribe(
                short_audio, trt_device=torch.device("cpu")
            )

        call_args = mock_trt_handler.return_value.infer.call_args[0][0]
        input_signal = call_args["input_signal"]
        assert input_signal.shape == (2, 16000)
        assert input_signal[:, 8000:].abs().sum() == 0
        assert call_args["input_signal_length"].tolist() == [1600, 8000]
