[![Test caul](https://github.com/ICIJ/datashare-python/actions/workflows/test_caul.yml/badge.svg)](https://github.com/ICIJ/datashare-python/actions/workflows/test_caul.yml)
![Supported Python versions](https://img.shields.io/badge/Python->%3D%203.12-blue)
[![PyPI](https://img.shields.io/pypi/v/caul)](https://pypi.org/project/caul/)

# caul
**Automatic speech recognition in Python**

***"Here's to Harry ... the best, bar none."***

Audiofile transcription using NVIDIA's [Parakeet](https://huggingface.co/nvidia/parakeet-tdt-0.6b-v3) family of multilingual 
models with fallback to [Whisper.cpp](https://https://github.com/ggml-org/whisper.cpp) for languages outside Parakeet's scope. 
Built with `uv` for package and project management. Installation's as simple as
```aiignore
cd caul
uv python install 3.12
uv sync --dev --extra cpu
```

A handler object can be instantiated and run on one or more audio file paths or 
directly on NumPy/Torch tensors, returning a list of `ASRHandlerResult` 
for each input. `transcriptions` contains a list of 
tuples of the form `(start_time, end_time, text_segment)` and `scores` a measure
of confidence in a transcription in the `range(0, -250)`:
```aiignore
>>> from .handler import ASRHandler
>>> handler = ASRHandler(models="parakeet")
>>> handler.startup()
>>> results = handler.transcribe("<...path to some audio file...>")
>>> print(results)
[ASRInferenceHandlerResult(transcription=[(0.0, 1.5, "We're spending too much time here.")], score=-250.0),
ASRInferenceHandlerResult(transcription=[(1.5, 2.9, "Stay a little longer.")], score=-250.0),
ASRInferenceHandlerResult(transcription=[(2.9, 4.0, "He'd kill us if he got the chance.")], score=-250.0)]
```