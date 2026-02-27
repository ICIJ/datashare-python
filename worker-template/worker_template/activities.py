from .classify import ClassifyDocs, CreateClassificationBatches
from .translate import CreateTranslationBatches, TranslateDocs
from .workflows import Pong

ACTIVITIES = [
    CreateTranslationBatches.create_translation_batches,
    CreateClassificationBatches.create_classification_batches,
    TranslateDocs.translate_docs,
    ClassifyDocs.classify_docs,
    Pong.pong,
]
