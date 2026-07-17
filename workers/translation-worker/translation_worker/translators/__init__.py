try:
    from .argos import ArgosTranslator
except ModuleNotFoundError:
    ArgosTranslator = None

try:
    from .hunyuan import HunyuanMtTranslator
except ModuleNotFoundError:
    HunyuanMtTranslator = None
