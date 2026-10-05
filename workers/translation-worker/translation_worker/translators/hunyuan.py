import gc
from collections.abc import Iterable
from typing import TYPE_CHECKING, Self

import langcodes
from datashare_python.objects import DatashareLanguage, Language

from ..config import TranslationWorkerConfig
from ..objects import HunyuanMtTranslatorConfig, TranslationModel
from ..processors import Translator

if TYPE_CHECKING:
    import torch


_CHINESE_LANGUAGES = {"zh", "yue"}


def _language_tag(language: Language) -> langcodes.Language:
    if isinstance(language, DatashareLanguage):
        return langcodes.get(language.alpha3)
    # IETF tags
    return langcodes.get(language)


def _message_template(text: str, source: Language, target: Language) -> dict[str, str]:
    source_tag, target_tag = _language_tag(source), _language_tag(target)
    is_chinese_pair = bool(
        {source_tag.language, target_tag.language} & _CHINESE_LANGUAGES
    )
    # tencent recommends using a chinese prompt when translating from chinese
    if is_chinese_pair:
        target_lang = target_tag.display_name("zh")
        context = (
            f"将以下文本翻译为{target_lang}，注意只需要输出翻译后的结果，不要额外解释："
            f"\n\n{text}"
        )
    else:
        target_lang = target_tag.display_name("en")
        context = (
            f"Translate the following segment into {target_lang}, without additional "
            f"explanation.\n\n{text}"
        )
    return {"role": "user", "content": context}


@Translator.register(TranslationModel.HUNYUAN)
class HunyuanMtTranslator(Translator):
    def __init__(
        self, config: HunyuanMtTranslatorConfig, device: "torch.device | str" = "cpu"
    ):
        super().__init__(config)

        self._tokenizer = None
        self._translator = None
        self._device = device

    @classmethod
    def _from_config(
        cls,
        config: HunyuanMtTranslatorConfig,
        device: "torch.device | str" = "cpu",
        **extras,  # noqa: ARG003
    ) -> Self:
        return cls(config, device=device)

    def load(
        self,
        source: Language,
        *,
        target: Language,
        worker_config: TranslationWorkerConfig,
    ) -> None:
        from transformers import AutoModelForCausalLM, AutoTokenizer  # noqa: PLC0415

        super().load(source, target=target, worker_config=worker_config)
        translator = AutoModelForCausalLM.from_pretrained(
            self._config.model_ref,
            device_map=self._config.device_map,
            torch_dtype=self._config.torch_dtype,
        )
        self._translator = translator
        self._tokenizer = AutoTokenizer.from_pretrained(
            self._config.model_ref, padding_side="left"
        )
        self._device = next(self._translator.parameters()).device

    def translate(self, texts: Iterable[str]) -> list[str]:
        conversations = [
            [_message_template(text, self._source, self._target)] for text in texts
        ]
        tokenized = self._tokenizer.apply_chat_template(
            conversations,
            tokenize=True,
            add_generation_prompt=False,
            return_tensors="pt",
            padding=True,
            return_dict=True,
        )
        input_ids = tokenized["input_ids"].to(self._device)
        attention_mask = tokenized["attention_mask"].to(self._device)
        outputs = self._translator.generate(
            input_ids,
            attention_mask=attention_mask,
            max_new_tokens=self._config.max_new_tokens,
            do_sample=self._config.do_sample,
            top_k=self._config.top_k,
            top_p=self._config.top_p,
            temperature=self._config.temperature,
            repetition_penalty=self._config.repetition_penalty,
        )
        # generate returns the prompt followed by the completion, only decode the
        # completion
        prompt_length = input_ids.shape[-1]
        return [
            self._tokenizer.decode(out[prompt_length:], skip_special_tokens=True)
            for out in outputs
        ]

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001
        import torch  # noqa: PLC0415

        self._tokenizer = None
        self._translator = None

        if torch.cuda.is_available():
            torch.cuda.empty_cache()

        gc.collect()
