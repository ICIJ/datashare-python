from unittest.mock import MagicMock, patch

import pytest
import torch
from caul.exception import TrtInputShapeError
from caul.trt.handler import TrtInferenceHandler

_IMPORT_TRT_PATH = "caul.trt.handler.import_trt"
_DTYPES_MAP_PATH = "caul.trt.handler._dtypes_map"


def _mock_tensor(shape: tuple) -> MagicMock:
    tensor = MagicMock()
    tensor.contiguous.return_value.to.return_value.shape = shape
    return tensor


def test_infer__raises_when_input_shape_outside_profile() -> None:
    engine = MagicMock()
    engine.get_tensor_profile_shape.return_value = (
        (1, 16000),
        (4, 160000),
        (4, 320000),
    )
    handler = TrtInferenceHandler(engine)
    handler._context = MagicMock()
    handler._context.set_input_shape.return_value = False

    with (
        patch(_IMPORT_TRT_PATH),
        patch(_DTYPES_MAP_PATH),
        pytest.raises(TrtInputShapeError, match=r"\(4, 480000\).*max=\(4, 320000\)"),
    ):
        handler.infer({"input_signal": _mock_tensor((4, 480000))})

    handler._context.execute_async_v3.assert_not_called()


def test_infer__casts_inputs_to_engine_dtype() -> None:
    engine = MagicMock()
    trt_int64 = object()
    engine.get_tensor_dtype.return_value = trt_int64
    engine.get_tensor_profile_shape.return_value = ((1,), (4,), (8,))
    handler = TrtInferenceHandler(engine)
    handler._context = MagicMock()
    # stop right after the input loop
    handler._context.set_input_shape.return_value = False
    lengths = _mock_tensor((4,))

    with (
        patch(_IMPORT_TRT_PATH),
        patch(_DTYPES_MAP_PATH, return_value={trt_int64: torch.int64}),
        pytest.raises(TrtInputShapeError),
    ):
        handler.infer({"input_signal_length": lengths})

    assert lengths.contiguous.return_value.to.call_args.kwargs["dtype"] == torch.int64
