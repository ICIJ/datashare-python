import json

import pytest
import requests
import spacy
from icij_common.test_utils import fail_if_exception
from ner_worker.constants import DATA_DIR
from ner_worker.objects_ import Category, NamedEntity, NlpTag, SpacySize
from ner_worker.spacy_.ner import SpacyPipelineCache, spacy_ner
from packaging.version import Version

_MODEL_PATH = DATA_DIR.joinpath("models.json")
_ALL_LANGUAGES = list(json.loads(_MODEL_PATH.read_text()))


@pytest.fixture(scope="session")
def test_spacy_pipeline_cache() -> SpacyPipelineCache:
    return SpacyPipelineCache(max_languages=1)


def test_spacy_provider_get_ner(test_spacy_pipeline_cache: SpacyPipelineCache) -> None:
    # Given
    language = "en"
    provider = test_spacy_pipeline_cache
    # When/Then
    msg = f"failed to load ner pipeline for {language}"
    with fail_if_exception(msg):
        provider.get_ner(language, model_size=SpacySize.SMALL)


def test_spacy_provider_get_sent_split(
    test_spacy_pipeline_cache: SpacyPipelineCache,
) -> None:
    # Given
    language = "en"
    model_size = SpacySize.SMALL
    provider = test_spacy_pipeline_cache
    # When/Then
    msg = f"failed to load sentence split pipeline for {language}"
    with fail_if_exception(msg):
        provider.get_sent_split(language, model_size=model_size)


@pytest.mark.skip
def test_spacy_model_compatibility() -> None:
    # Given
    compatibility_url = (
        "https://raw.githubusercontent.com/explosion/spacy-models/"
        "master/compatibility.json"
    )
    r = requests.get(compatibility_url)
    r.raise_for_status()
    version = Version(spacy.__version__)
    version = f"{version.major}.{version.minor}"
    version_compatibility = r.json()["spacy"][version]
    models = json.loads(_MODEL_PATH.read_text())

    # Then
    for model in models.values():
        compatible_versions = version_compatibility[model["model"]]
        assert model["version"] in compatible_versions


@pytest.mark.parametrize(
    ("categories", "expected_entities"),
    [
        (
            [Category.LOC, Category.PER, Category.ORG],
            [
                [
                    NlpTag(start=57, mention="Dan", category=Category.PER),
                    NlpTag(mention="Paris", category=Category.LOC, start=93),
                    NlpTag(mention="Paris", category=Category.LOC, start=103),
                    NlpTag(mention="Intel", category=Category.ORG, start=161),
                ],
                [],
            ],
        ),
        (
            [Category.LOC],
            [
                [
                    NlpTag(mention="Paris", category=Category.LOC, start=93),
                    NlpTag(mention="Paris", category=Category.LOC, start=103),
                ],
                [],
            ],
        ),
    ],
)
async def test_spacy_ner(
    categories: list[Category] | None,
    expected_entities: list[list[NamedEntity]],
    text_0: str,
    text_1: str,
) -> None:
    # Given
    texts = [text_0, text_1]
    language = "en"
    provider = SpacyPipelineCache(max_languages=1)
    model_size = SpacySize.SMALL
    ner = provider.get_ner(language, model_size=model_size)
    sent_split = provider.get_ner(language, model_size=model_size)

    # When
    entities = [
        e
        async for e in spacy_ner(
            texts,
            ner,
            categories=categories,
            sent_split=sent_split,
            n_process=1,
            batch_size=2,
        )
    ]

    # Then
    assert entities == expected_entities
