from pathlib import Path


class MissingModelSpecificationException(Exception):
    """Raise if referencing a missing model"""


class UnsupportedModelException(Exception):
    """Raise if an unsupported model type is passed"""


class MissingFireRedAsr2OutputDirException(Exception):
    """Raise if no file path is available to store FireRedAsr2 inputs at"""


class LanguageInputMismatchException(Exception):
    """Raise when inputs and input languages to ASRHandler.transcribe don't align"""


class MissingTokenizerException(Exception):
    """Raise if no tokenizer or tokenizer path provided"""


class UnreadableAudio(Exception):
    def __init__(self, path: Path):
        msg = f"failed to read audio file at {path}"
        super().__init__(msg)


class TrtEngineLoadError(Exception):
    def __init__(self, path: Path | str, trt_version: str):
        msg = (
            f"failed to deserialize TensorRT engine at {path} with TensorRT runtime"
            f" {trt_version}; the engine was likely built with a different TensorRT"
            f" version and must be rebuilt (see TensorRT logs above for details)"
        )
        super().__init__(msg)


class TrtInputShapeError(Exception):
    def __init__(self, tensor_name: str, shape: tuple, profile: tuple):
        min_shape, opt_shape, max_shape = (tuple(s) for s in profile)
        msg = (
            f"input {tensor_name!r} has shape {tuple(shape)}, outside TRT"
            f" engine's optimization profile (min={min_shape}, opt={opt_shape},"
            f" max={max_shape})."
        )
        super().__init__(msg)
