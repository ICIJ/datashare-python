from types import ModuleType


def import_trt() -> ModuleType:
    """Import TensorRT, preferring the lean runtime over the full package."""
    try:
        import tensorrt_lean as trt  # noqa: PLC0415
    except ImportError:
        import tensorrt as trt  # noqa: PLC0415

    return trt
