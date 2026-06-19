try:
    from .argos import ArgosTranslator
except ImportError:
    ArgosTranslator = None

try:
    from .hunyuan import HunyuanMtTranslator
except ImportError:
    HunyuanMtTranslator = None
