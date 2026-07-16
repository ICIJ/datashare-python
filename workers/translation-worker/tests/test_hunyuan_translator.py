# ruff: noqa: ANN001, ANN202
import sys
from unittest.mock import MagicMock, patch

from translation_worker.config import DEFAULT_HUNYUAN_MODEL_REF
from translation_worker.translators.hunyuan import HunyuanMtTranslator

from tests.conftest import DS_CHINESE, DS_ENGLISH

TEXT_TO_TRANSLATE = "text to translate"
MORE_TEXT_TO_TRANSLATE = "more text to translate"
TRANSLATED_TEXT = "translated text"


def _make_config(max_new_tokens: int = 2048, torch_dtype: str = "float32") -> MagicMock:
    config = MagicMock()

    config.max_new_tokens = max_new_tokens
    config.torch_dtype = torch_dtype

    return config


def _translator(device: str = "cpu", **config_kwargs) -> HunyuanMtTranslator:
    translator = HunyuanMtTranslator(_make_config(**config_kwargs), device=device)

    translator._source = DS_CHINESE
    translator._target = DS_ENGLISH

    return translator


def _dummy_transformers() -> MagicMock:
    mock = MagicMock()
    mock.AutoTokenizer = MagicMock()
    mock.AutoModelForCausalLM = MagicMock()
    return mock


def test__translate__wraps_each_text_in_prompt() -> None:
    translator = _translator()
    translator._tokenizer = MagicMock()
    translator._translator = MagicMock()
    translator.translate([TEXT_TO_TRANSLATE, MORE_TEXT_TO_TRANSLATE])
    translator._tokenizer.apply_chat_template.assert_called_once_with(
        [
            [
                {
                    "role": "user",
                    "content": f"Translate into English, "
                    f"without additional explanation: "
                    f"{TEXT_TO_TRANSLATE}",
                }
            ],
            [
                {
                    "role": "user",
                    "content": f"Translate into English, "
                    f"without additional explanation: "
                    f"{MORE_TEXT_TO_TRANSLATE}",
                }
            ],
        ],
        tokenize=True,
        add_generation_prompt=False,
        return_tensors="pt",
        padding=True,
    )


def test__translate__moves_tokenized_input_to_device_before_generation() -> None:
    translator = _translator(device="cpu")
    mock_tokenizer = MagicMock()
    mock_model = MagicMock()
    translator._tokenizer = mock_tokenizer
    translator._translator = mock_model
    tokenized = MagicMock()
    mock_tokenizer.apply_chat_template.return_value = tokenized
    translator.translate([TEXT_TO_TRANSLATE])
    input_ids = tokenized["input_ids"]
    input_ids.to.assert_called_once_with("cpu")
    mock_model.generate.assert_called_once_with(
        input_ids.to.return_value, max_new_tokens=2048
    )


def test__translate__decodes_first_element_of_generate_output() -> None:
    translator = _translator()
    mock_tokenizer = MagicMock()
    mock_model = MagicMock()
    translator._tokenizer = mock_tokenizer
    translator._translator = mock_model
    first_output = MagicMock()
    mock_model.generate.return_value = [first_output]
    mock_tokenizer.decode.return_value = TRANSLATED_TEXT
    result = translator.translate([TEXT_TO_TRANSLATE])
    mock_tokenizer.decode.assert_called_once_with(
        first_output, skip_special_tokens=True
    )
    assert result == [TRANSLATED_TEXT]


def test__load__initialises_tokenizer_and_model_from_config_model_ref_with_device_map_and_dtype() -> (  # noqa: E501
    None
):
    translator = _translator()
    translator._config.model_ref = DEFAULT_HUNYUAN_MODEL_REF
    translator._config.torch_dtype = "bfloat16"
    translator._config.device_map = "cuda:0"
    dummy_tf = _dummy_transformers()
    with (
        patch.dict(sys.modules, {"transformers": dummy_tf}),
    ):
        translator.load(MagicMock(), target=MagicMock(), worker_config=MagicMock())

    dummy_tf.AutoTokenizer.from_pretrained.assert_called_once_with(
        DEFAULT_HUNYUAN_MODEL_REF
    )

    dummy_tf.AutoModelForCausalLM.from_pretrained.assert_called_once_with(
        DEFAULT_HUNYUAN_MODEL_REF,
        device_map="cuda:0",
        torch_dtype="bfloat16",
    )
