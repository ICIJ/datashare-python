import gc
from collections.abc import Iterable
from typing import TYPE_CHECKING, Self

from datashare_python.objects import Language

from ..config import HunyuanMtTranslatorConfig, TranslationWorkerConfig
from ..constants import HUNYUAN_SHARED_KEY
from ..objects import TranslationModel
from ..processors import Translator

if TYPE_CHECKING:
    import torch


def _message_template(text: str, target_lang: str) -> dict[str, str]:
    context = f"Translate into {target_lang}, without additional explanation: {text}"
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

    def _load(
        self,
        source: Language,
        *,
        target: Language,
        worker_config: TranslationWorkerConfig,
    ) -> None:
        from datashare_python.dependencies import (  # noqa: PLC0415
            lifespan_shared_resources,
        )
        from datashare_python.exceptions import (  # noqa: PLC0415
            DependencyInjectionError,
        )
        from transformers import AutoModelForCausalLM, AutoTokenizer  # noqa: PLC0415

        super()._load(source, target=target, worker_config=worker_config)

        try:
            shared = lifespan_shared_resources()
        except DependencyInjectionError:
            shared = None

        translator = (
            shared.get_resource(HUNYUAN_SHARED_KEY) if shared is not None else None
        )

        if translator is None:
            translator = AutoModelForCausalLM.from_pretrained(
                self._config.model_ref,
                device_map=self._config.device_map,
                torch_dtype=self._config.torch_dtype,
            )
            if shared is not None:
                shared.set_resource(HUNYUAN_SHARED_KEY, translator)

        self._translator = translator
        self._tokenizer = AutoTokenizer.from_pretrained(self._config.model_ref)
        self._device = next(self._translator.parameters()).device

    def translate(self, texts: Iterable[str]) -> list[str]:
        target_lang = self._target.title()
        conversations = [[_message_template(text, target_lang)] for text in texts]
        tokenized = self._tokenizer.apply_chat_template(
            conversations,
            tokenize=True,
            add_generation_prompt=False,
            return_tensors="pt",
            padding=True,
        )
        input_ids = tokenized["input_ids"] if hasattr(tokenized, "keys") else tokenized
        outputs = self._translator.generate(
            input_ids.to(self._device), max_new_tokens=self._config.max_new_tokens
        )
        return [
            self._tokenizer.decode(out, skip_special_tokens=True) for out in outputs
        ]

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001
        import torch  # noqa: PLC0415

        self._tokenizer = None
        self._translator = None

        if torch.cuda.is_available():
            torch.cuda.empty_cache()

        gc.collect()
