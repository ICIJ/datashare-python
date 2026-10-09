import base64
import math
from collections import OrderedDict
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TYPE_CHECKING

from caul_core import (
    WHISPER_TRT_ENCODER_INPUT_FEATURES,
    WHISPER_TRT_ENCODER_INPUT_LENGTHS,
    WHISPER_TRT_ENCODER_POSITION_IDS,
    WHISPER_TRT_EOT_TOKEN,
    WHISPER_TRT_MAX_MEL_PADDING_LEN,
    WHISPER_TRT_MAX_NEW_TOKENS,
    WHISPER_TRT_PAD_TOKEN_ID,
    WHISPER_TRT_PATTERN_STR,
    WHISPER_TRT_PROMPT_PREFIX,
    WHISPER_TRT_SPECIAL_TOKENS,
    WHISPER_TRT_WORLD_SIZE,
    ASRModel,
    ASRResult,
    Error,
    InferenceRunner,
    ProcessedAudioSegment,
    TorchDevice,
    TrtLlmDecoderConfig,
    TrtLlmEncoderConfig,
    WhisperTrtInferenceRunnerConfig,
)
from icij_common.registrable import FromConfig

from ...exception import MissingTokenizerException
from .trt_inference import TrtInferenceMixin

if TYPE_CHECKING:
    import tiktoken
    import torch

    try:
        from tensorrt_llm.runtime import GenerationSession
        from tensorrt_llm.runtime.session import Session
    except ModuleNotFoundError:
        Session = None
        GenerationSession = None


def _encoder_factory(encoder_path: Path | str) -> "Callable[[], Session]":
    def _load_encoder() -> "Session":
        from tensorrt_llm.runtime.session import (
            Session,
        )

        with open(encoder_path, "rb") as f:
            return Session.from_serialized_engine(f.read())

    return _load_encoder


def _decoder_factory(
    decoder_path: Path | str,
    decoder_config: TrtLlmDecoderConfig | None = None,
) -> "Callable[[torch.cuda.Stream | None], GenerationSession]":
    if decoder_config is None:
        decoder_config = TrtLlmDecoderConfig()

    def _load_decoder(stream: "torch.cuda.Stream | None" = None) -> "GenerationSession":
        from tensorrt_llm import (
            Mapping,
            mpi_rank,
        )
        from tensorrt_llm.runtime import (
            GenerationSession,
        )

        with open(decoder_path, "rb") as f:
            decoder_engine = f.read()

        runtime_rank = mpi_rank()
        runtime_mapping = Mapping(WHISPER_TRT_WORLD_SIZE, runtime_rank)

        return GenerationSession(
            decoder_config.to_model_config(),
            decoder_engine,
            runtime_mapping,
            stream=stream,
            debug_mode=decoder_config.debug_mode,
        )

    return _load_decoder


def _get_tiktoken_tokenizer(vocab_path: Path) -> "tiktoken.Encoding":
    import tiktoken

    with open(vocab_path) as f:
        ranks = {
            base64.b64decode(token): int(rank)
            for token, rank in (line.split() for line in f if line)
        }

    n_vocab = len(ranks)

    return tiktoken.Encoding(
        name=vocab_path.name,
        explicit_n_vocab=n_vocab,
        pat_str=WHISPER_TRT_PATTERN_STR,
        mergeable_ranks=ranks,
        special_tokens=WHISPER_TRT_SPECIAL_TOKENS,
    )


def _remove_tensor_padding(
    input_tensor: "torch.Tensor",
    input_tensor_lens: "torch.Tensor" = None,
    pad_value: int = None,
) -> "torch.Tensor":
    import torch

    if pad_value and input_tensor_lens is None:
        # Create a mask for all non-pad tokens
        mask = input_tensor != pad_value

        # Apply the mask to input_tensor to remove pad tokens
        output_tensor = input_tensor[mask].view(1, -1)

    else:
        # Initialize a list to collect valid sequences
        valid_sequences = []

        for i in range(input_tensor.shape[0]):
            valid_len = input_tensor_lens[i]
            valid_sequences.append(input_tensor[i, :valid_len])

        # Concatenate all valid sequences along the batch dimension
        output_tensor = torch.cat(valid_sequences, dim=0)
    return output_tensor


@InferenceRunner.register(ASRModel.WHISPER_TRT)
class WhisperTrtInferenceRunner(InferenceRunner, TrtInferenceMixin):
    def __init__(
        self,
        encoder_factory: Callable[[], "Session"],
        decoder_factory: Callable[["torch.cuda.Stream | None"], "GenerationSession"],
        *,
        tokenizer: "tiktoken.Encoding" = None,
        tokenizer_vocab_path: Path | str = None,
        encoder_config: TrtLlmEncoderConfig = None,
        decoder_config: TrtLlmDecoderConfig = None,
        max_new_tokens: int = WHISPER_TRT_MAX_NEW_TOKENS,
        prompt_prefix: str = WHISPER_TRT_PROMPT_PREFIX,
        return_timestamps: bool = True,
        max_mel_padding_len: int = WHISPER_TRT_MAX_MEL_PADDING_LEN,
        device: TorchDevice = TorchDevice.CPU,
    ):
        if tokenizer is None and tokenizer_vocab_path is None:
            raise MissingTokenizerException(
                "Either tokenizer or tokenizer_path must be specified."
            )

        import torch

        InferenceRunner.__init__(self, device=device)
        TrtInferenceMixin.__init__(self)

        if encoder_config is None:
            encoder_config = TrtLlmEncoderConfig()

        if decoder_config is None:
            decoder_config = TrtLlmDecoderConfig()

        self._tokenizer_vocab_path = (
            Path(tokenizer_vocab_path)
            if isinstance(tokenizer_vocab_path, str)
            else tokenizer_vocab_path
        )
        self._encoder_factory = encoder_factory
        self._decoder_factory = decoder_factory
        self._encoder_config = encoder_config
        self._decoder_config = decoder_config
        self._max_new_tokens = max_new_tokens
        self._return_timestamps = return_timestamps
        self._max_mel_padding_len = max_mel_padding_len

        # tokenizer
        self._tokenizer = (
            tokenizer
            if tokenizer is not None
            else _get_tiktoken_tokenizer(tokenizer_vocab_path)
        )

        self._prompt_ids = torch.tensor(
            self._tokenizer.encode(
                prompt_prefix, allowed_special=self._tokenizer.special_tokens_set
            )
        )
        self._eot_token_id = self._tokenizer.encode(
            WHISPER_TRT_EOT_TOKEN, allowed_special=self._tokenizer.special_tokens_set
        )[0]

    @classmethod
    def _from_config(
        cls,
        config: WhisperTrtInferenceRunnerConfig,
        **extras,
    ) -> FromConfig:
        return cls(
            encoder_factory=_encoder_factory(config.encoder_path),
            decoder_factory=_decoder_factory(
                config.decoder_path, config.decoder_config
            ),
            encoder_config=config.encoder_config,
            decoder_config=config.decoder_config,
            prompt_prefix=config.prompt_prefix,
            return_timestamps=config.return_timestamps,
            max_mel_padding_len=config.max_mel_padding_len,
            **extras,
        )

    def __enter__(self, stream: "torch.cuda.stream | None" = None):
        self._encoder = self._encoder_factory()
        self._decoder = self._decoder_factory(stream)
        return self

    def process(
        self, inputs: Iterable[tuple[ProcessedAudioSegment, ...]], *args, **kwargs
    ) -> Iterable[ASRResult | Error]:
        import torch

        for input_batch in inputs:
            if not input_batch or not hasattr(input_batch[0], "tensor"):
                continue

            audio_inputs = torch.stack(
                [
                    torch.nn.functional.pad(
                        preprocessed_input.tensor,
                        (
                            0,
                            self._max_mel_padding_len
                            - preprocessed_input.tensor.shape[-1],
                        ),
                    )
                    for preprocessed_input in input_batch
                ]
            ).to(self._torch_device)

            # capture original (unpadded) time lengths before padding
            audio_inputs_lens = torch.tensor(
                [t.shape[-1] for t in audio_inputs], dtype=torch.int32
            ).to(self._torch_device)

            # ravel tensors for GPU
            audio_inputs = audio_inputs.contiguous()
            batch_size, seq_len = audio_inputs.shape[0], audio_inputs.shape[-1]

            # generate position ids
            position_ids = (
                torch.arange(
                    math.ceil(seq_len / self._encoder_config.downsampling_factor),
                    dtype=torch.int32,
                    device=self._torch_device,
                )
                .expand(batch_size, -1)
                .contiguous()
            )

            encoder_output, encoder_output_lens = self._run_encoder(
                audio_inputs, audio_inputs_lens, position_ids
            )

            decoder_output = self._run_decoder(encoder_output, encoder_output_lens)

            for decoder_out_idx, decoder_out in enumerate(
                decoder_output.numpy().tolist()
            ):
                transcription = self._decode_model_output(decoder_out[0])

                input_ordering_idx = input_batch[decoder_out_idx].metadata.index
                yield ASRResult(transcription=transcription, index=input_ordering_idx)

    def _run_encoder(
        self,
        audio_inputs: "torch.Tensor",
        audio_inputs_lens: "torch.Tensor",
        position_ids: "torch.Tensor",
        stream: "torch.cuda.Stream | None" = None,
    ) -> tuple["torch.Tensor", "torch.Tensor"]:
        import torch
        from tensorrt_llm._utils import (
            torch_dtype_to_trt,
            trt_dtype_to_torch,
        )
        from tensorrt_llm.runtime.session import (
            TensorInfo,
        )

        encoder_input_list = [
            TensorInfo(
                WHISPER_TRT_ENCODER_INPUT_FEATURES,
                torch_dtype_to_trt(audio_inputs.dtype),
                audio_inputs.shape,
            ),
            TensorInfo(
                WHISPER_TRT_ENCODER_INPUT_LENGTHS,
                torch_dtype_to_trt(audio_inputs_lens.dtype),
                audio_inputs_lens.shape,
            ),
            TensorInfo(
                WHISPER_TRT_ENCODER_POSITION_IDS,
                torch_dtype_to_trt(position_ids.dtype),
                position_ids.shape,
            ),
        ]

        encoder_output_shapes = self._encoder.infer_shapes(encoder_input_list)

        encoder_outputs = {
            t.name: torch.empty(
                tuple(t.shape),
                dtype=trt_dtype_to_torch(t.dtype),
                device=self._torch_device,
            )
            for t in encoder_output_shapes
        }

        if stream is None:
            stream = torch.cuda.current_stream()

        encoder_inputs = OrderedDict()
        encoder_inputs[WHISPER_TRT_ENCODER_INPUT_FEATURES] = audio_inputs
        encoder_inputs[WHISPER_TRT_ENCODER_INPUT_LENGTHS] = audio_inputs_lens
        encoder_inputs[WHISPER_TRT_ENCODER_POSITION_IDS] = position_ids

        self._encoder.run(
            inputs=encoder_inputs, outputs=encoder_outputs, stream=stream.cuda_stream
        )

        stream.synchronize()

        encoder_output = encoder_outputs["encoder_output"]
        encoder_output_lens = (
            audio_inputs_lens // self._encoder_config.downsampling_factor
        )

        return encoder_output, encoder_output_lens

    def _run_decoder(
        self, decoder_inputs: "torch.Tensor", decoder_inputs_lens: "torch.Tensor"
    ) -> "torch.Tensor":
        import torch
        from tensorrt_llm.runtime import (
            SamplingConfig,
        )

        decoder_inputs_max_len = decoder_inputs_lens.max().item()
        prompt_ids_max_len = self._prompt_ids.shape[-1]
        batch_size = decoder_inputs_lens.shape[0]
        prompt_inputs = (
            self._prompt_ids.unsqueeze(0).repeat(batch_size, 1).type(torch.int32).cuda()
        )
        prompt_inputs_lens = torch.tensor([prompt_ids_max_len] * batch_size).cuda()
        cross_attention_mask = (
            torch.ones(
                [
                    batch_size,
                    prompt_ids_max_len + self._max_new_tokens,
                    int(decoder_inputs_max_len),
                ]
            )
            .int()
            .cuda()
            if self._decoder_config.cross_attention
            else None
        )

        if self._decoder_config.remove_input_padding:
            prompt_inputs = _remove_tensor_padding(
                prompt_inputs, pad_value=WHISPER_TRT_PAD_TOKEN_ID
            )
            if decoder_inputs.dim() == 3:
                decoder_inputs_lens = decoder_inputs.new_full(
                    (decoder_inputs.shape[0],),
                    decoder_inputs.shape[1],
                    dtype=torch.int32,
                )

                decoder_inputs = _remove_tensor_padding(
                    decoder_inputs, decoder_inputs_lens
                )

        sampling_config = SamplingConfig(
            end_id=self._eot_token_id,
            pad_id=self._eot_token_id,
            num_beams=self._decoder_config.beam_width,
        )

        self._decoder.setup(
            decoder_inputs_lens.size(0),
            prompt_ids_max_len,
            self._max_new_tokens,
            beam_width=self._decoder_config.beam_width,
            encoder_max_input_length=decoder_inputs_max_len,
        )

        decoder_output = self._decoder.decode(
            prompt_inputs,
            prompt_inputs_lens,
            sampling_config,
            encoder_output=decoder_inputs,
            encoder_input_lengths=decoder_inputs_lens,
            cross_attention_mask=cross_attention_mask,
        )

        # get the list of int from output_ids tensor
        return decoder_output.cpu()

    def _decode_model_output(self, decoder_output: list) -> list[tuple]:
        # TODO: By default, Whisper TRT doesn't return timestamps. Need to implement a
        #   custom alignment method
        text = self._tokenizer.decode(decoder_output).strip()
        return [(0.0, 0.0, text)] if text else []
