from enum import Enum, EnumMeta, unique
from functools import lru_cache
from typing import Any

from ..objects_ import Category

_CONLL = ("LOC", "MISC", "ORG", "PER")
_KOREAN = ("DT", "LC", "OG", "PS", "QT", "TI")
_LITHUANIAN = ("GPE", "LOC", "ORG", "PERSON", "PRODUCT", "TIME")
_ONTO_NOTES_5 = (
    "CARDINAL",
    "DATE",
    "EVENT",
    "FAC",
    "GPE",
    "LANGUAGE",
    "LAW",
    "LOC",
    "MONEY",
    "NORP",
    "ORDINAL",
    "ORG",
    "PERCENT",
    "PERSON",
    "PRODUCT",
    "QUANTITY",
    "TIME",
    "WORK_OF_ART",
)
_GREEK = ("EVENT", "GPE", "LOC", "ORG", "PERSON", "PRODUCT")
_CROATIAN_SLOVENIAN = ("DERIV_PER", "LOC", "MISC", "ORG", "PER")
_JAPANESE = (
    "CARDINAL",
    "DATE",
    "EVENT",
    "FAC",
    "GPE",
    "LANGUAGE",
    "LAW",
    "LOC",
    "MONEY",
    "MOVEMENT",
    "NORP",
    "ORDINAL",
    "ORG",
    "PERCENT",
    "PERSON",
    "PET_NAME",
    "PHONE",
    "PRODUCT",
    "QUANTITY",
    "TIME",
    "TITLE_AFFIX",
    "WORK_OF_ART",
)
_NORWEGIAN = ("DRV", "EVT", "GPE_LOC", "GPE_ORG", "LOC", "MISC", "ORG", "PER", "PROD")
_POLISH = ("date", "geogName", "orgName", "persName", "placeName", "time")
_ROMANIAN = (
    "DATETIME",
    "EVENT",
    "FACILITY",
    "GPE",
    "LANGUAGE",
    "LOC",
    "MONEY",
    "NAT_REL_POL",
    "NUMERIC_VALUE",
    "ORDINAL",
    "ORGANIZATION",
    "PERIOD",
    "PERSON",
    "PRODUCT",
    "QUANTITY",
    "WORK_OF_ART",
)
_RUSSIAN = ("LOC", "ORG", "PER")
_SWEDISH = ("EVN", "LOC", "MSR", "OBJ", "ORG", "PRS", "TME", "WRK")


class SortedValuesEnumMeta(EnumMeta):
    def __call__(cls, value: Any, *args, **kw):
        value = tuple(sorted(value))
        return super().__call__(value, *args, **kw)


@unique
class NERLabelScheme(tuple, Enum, metaclass=SortedValuesEnumMeta):
    CONLL = _CONLL
    CROATIAN_SLOVENIAN = _CROATIAN_SLOVENIAN
    GREEK = _GREEK
    JAPANESE = _JAPANESE
    LITHUANIAN = _LITHUANIAN
    NORWEGIAN = _NORWEGIAN
    ONTO_NOTES_5 = _ONTO_NOTES_5
    POLISH = _POLISH
    ROMANIAN = _ROMANIAN
    RUSSIAN = _RUSSIAN
    SWEDISH = _SWEDISH

    def __new__(cls, value: tuple[str, ...]):
        obj = super().__new__(cls, tuple(sorted(value)))
        return obj

    @lru_cache
    def to_spacy(self, spacy_label: str) -> Category | None:
        match self:
            case NERLabelScheme.CONLL:
                return conll_label_to_ds(spacy_label)
            case NERLabelScheme.CROATIAN_SLOVENIAN:
                return croatian_slovenian_label_to_ds(spacy_label)
            case NERLabelScheme.GREEK:
                return greek_label_to_ds(spacy_label)
            case NERLabelScheme.JAPANESE:
                return japanese_label_to_ds(spacy_label)
            case NERLabelScheme.LITHUANIAN:
                return lithuanian_label_to_ds(spacy_label)
            case NERLabelScheme.NORWEGIAN:
                return norwegian_label_to_ds(spacy_label)
            case NERLabelScheme.ONTO_NOTES_5:
                return onto_notes_5_label_to_ds(spacy_label)
            case NERLabelScheme.POLISH:
                return polish_label_to_ds(spacy_label)
            case NERLabelScheme.ROMANIAN:
                return romanian_label_to_ds(spacy_label)
            case NERLabelScheme.RUSSIAN:
                return russian_label_to_ds(spacy_label)
            case _:
                raise ValueError(f"Unexpected value {self}")


def conll_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC":
            return Category.LOC
        case "ORG":
            return Category.ORG
        case "PER":
            return Category.PER
        case "MISC":
            return None
        case _:
            raise ValueError(f"Unexpected value {label}")


def croatian_slovenian_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC":
            return Category.LOC
        case "ORG":
            return Category.ORG
        case "PER" | "DERIV_PER":
            return Category.PER
        case "MISC":
            return None
        case _:
            raise ValueError(f"Unexpected value {label}")


def greek_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC" | "GPE" | "EVENT":
            return Category.LOC
        case "ORG":
            return Category.ORG
        case "PERSON":
            return Category.PER
        case "PRODUCT":
            return None
        case _:
            raise ValueError(f"Unexpected value {label}")


def japanese_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC" | "GPE" | "FAC" | "EVENT":
            return Category.LOC
        case "CARDINAL" | "ORDINAL" | "PERCENT" | "QUANTITY":
            return Category.NUM
        case "DATE" | "TIME":
            return Category.DATE
        case "PERSON":
            return Category.PER
        case "ORG" | "MOVEMENT" | "NORP":
            return Category.ORG
        case "MONEY":
            return Category.MONEY
        case (
            "LANGUAGE"
            | "PRODUCT"
            | "LAW"
            | "PET_NAME"
            | "PHONE"
            | "TITLE_AFFIX"
            | "WORK_OF_ART"
        ):
            return None
        case _:
            raise ValueError(f"Unexpected value {label}")


def lithuanian_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC" | "GPE":
            return Category.LOC
        case "ORG":
            return Category.ORG
        case "PERSON":
            return Category.PER
        case "PRODUCT":
            return None
        case "TIME":
            return Category.DATE
        case _:
            raise ValueError(f"Unexpected value {label}")


def norwegian_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC" | "GPE_LOC" | "EVT":
            return Category.LOC
        case "ORG" | "GPE_ORG":
            return Category.ORG
        case "PER":
            return Category.PER
        case "DRV" | "MISC" | "PROD":  # This could probably be person ?
            return None
    return None


def onto_notes_5_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC" | "GPE" | "FAC" | "EVENT":
            return Category.LOC
        case "CARDINAL" | "ORDINAL" | "PERCENT" | "QUANTITY":
            return Category.NUM
        case "DATE" | "TIME":
            return Category.DATE
        case "PERSON":
            return Category.PER
        case "ORG" | "NORP":
            return Category.ORG
        case "MONEY":
            return Category.MONEY
        case "LANGUAGE" | "PRODUCT" | "LAW" | "WORK_OF_ART":
            return None
        case _:
            raise ValueError(f"Unexpected value {label}")


def polish_label_to_ds(label: str) -> Category | None:
    match label:
        case "date" | "time":
            return Category.DATE
        case "geogName" | "placeName":
            return Category.LOC
        case "orgName":
            return Category.ORG
        case "persName":
            return Category.PER
        case _:
            raise ValueError(f"Unexpected value {label}")


def romanian_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC" | "GPE" | "FACILITY" | "EVENT":
            return Category.LOC
        case "DATETIME" | "PERIOD":
            return Category.DATE
        case "MONEY":
            return Category.MONEY
        case "ORGANIZATION" | "NAT_REL_POL":
            return Category.ORG
        case "PERSON":
            return Category.PER
        case "NUMERIC_VALUE" | "ORDINAL":
            return Category.NUM
        case "LANGUAGE" | "PRODUCT" | "QUANTITY" | "WORK_OF_ART":
            return None
        case _:
            raise ValueError(f"Unexpected value {label}")


def russian_label_to_ds(label: str) -> Category | None:
    match label:
        case "LOC":
            return Category.LOC
        case "ORG":
            return Category.ORG
        case "PER":
            return Category.PER
        case _:
            raise ValueError(f"Unexpected value {label}")
