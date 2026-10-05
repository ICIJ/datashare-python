# ruff: noqa: ANN001, ANN202
import sys
from unittest.mock import MagicMock, patch

import pytest
from translation_worker.objects import (
    DEFAULT_HUNYUAN_MODEL_REF,
)
from translation_worker.translators.hunyuan import (
    HunyuanMtTranslator,
    _message_template,
)

from tests.conftest import DS_CHINESE, DS_ENGLISH, DS_FRENCH

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
                    "content": "将以下文本翻译为英语，"
                    "注意只需要输出翻译后的结果，不要额外解释：\n\n"
                    f"{TEXT_TO_TRANSLATE}",
                }
            ],
            [
                {
                    "role": "user",
                    "content": "将以下文本翻译为英语，"
                    "注意只需要输出翻译后的结果，不要额外解释：\n\n"
                    f"{MORE_TEXT_TO_TRANSLATE}",
                }
            ],
        ],
        tokenize=True,
        add_generation_prompt=False,
        return_tensors="pt",
        padding=True,
        return_dict=True,
    )


def test__translate__moves_tokenized_input_and_mask_to_device_before_generation() -> (
    None
):
    translator = _translator(device="cpu")
    mock_tokenizer = MagicMock()
    mock_model = MagicMock()
    translator._tokenizer = mock_tokenizer
    translator._translator = mock_model
    tokenized = {"input_ids": MagicMock(), "attention_mask": MagicMock()}
    mock_tokenizer.apply_chat_template.return_value = tokenized
    translator.translate([TEXT_TO_TRANSLATE])
    input_ids = tokenized["input_ids"]
    attention_mask = tokenized["attention_mask"]
    input_ids.to.assert_called_once_with("cpu")
    attention_mask.to.assert_called_once_with("cpu")
    _, kwargs = mock_model.generate.call_args
    assert mock_model.generate.call_args.args == (input_ids.to.return_value,)
    assert kwargs["attention_mask"] is attention_mask.to.return_value


def test__translate__decodes_only_generated_tokens() -> None:
    translator = _translator()
    mock_tokenizer = MagicMock()
    mock_model = MagicMock()
    translator._tokenizer = mock_tokenizer
    translator._translator = mock_model
    prompt_length = 3
    input_ids = mock_tokenizer.apply_chat_template.return_value["input_ids"]
    input_ids.to.return_value.shape = (1, prompt_length)
    prompt_and_completion = list(range(prompt_length + 2))
    mock_model.generate.return_value = [prompt_and_completion]
    mock_tokenizer.decode.return_value = TRANSLATED_TEXT
    result = translator.translate([TEXT_TO_TRANSLATE])
    mock_tokenizer.decode.assert_called_once_with(
        prompt_and_completion[prompt_length:], skip_special_tokens=True
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
        DEFAULT_HUNYUAN_MODEL_REF, padding_side="left"
    )

    dummy_tf.AutoModelForCausalLM.from_pretrained.assert_called_once_with(
        DEFAULT_HUNYUAN_MODEL_REF,
        device_map="cuda:0",
        torch_dtype="bfloat16",
    )


_ZH_PROMPT = "将以下文本翻译为{}，注意只需要输出翻译后的结果，不要额外解释：\n\n{}"
_XX_PROMPT = (
    "Translate the following segment into {}, without additional explanation.\n\n{}"  # noqa: E501
)


@pytest.mark.parametrize(
    ("source", "target", "expected"),
    [
        (DS_CHINESE, DS_ENGLISH, _ZH_PROMPT.format("英语", TEXT_TO_TRANSLATE)),
        (DS_ENGLISH, DS_CHINESE, _ZH_PROMPT.format("中文", TEXT_TO_TRANSLATE)),
        ("zh-Hant", "en", _ZH_PROMPT.format("英语", TEXT_TO_TRANSLATE)),
        ("yue", "en", _ZH_PROMPT.format("英语", TEXT_TO_TRANSLATE)),
        (DS_FRENCH, DS_ENGLISH, _XX_PROMPT.format("English", TEXT_TO_TRANSLATE)),
        (
            "fr",
            "en-US",
            _XX_PROMPT.format("English (United States)", TEXT_TO_TRANSLATE),
        ),
    ],
)
def test__message_template__picks_prompt_from_language_pair(
    source: str, target: str, expected: str
) -> None:
    message = _message_template(TEXT_TO_TRANSLATE, source, target)
    assert message == {"role": "user", "content": expected}
