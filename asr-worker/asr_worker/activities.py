import torchaudio
from caul.model_handlers.helpers import ParakeetModelHandlerResult
from caul.configs.parakeet import ParakeetConfig
from caul.tasks.preprocessing.helpers import PreprocessedInput
from temporalio import activity


class ASRActivities:
    """Contains activity definitions as well as reference to models"""

    def __init__(self):
        # TODO: Eventually this may include whisper, which will
        #  then require passing language_map
        self.asr_handler = ParakeetConfig(return_tensors=False).handler_from_config()

        # load models
        self.asr_handler.startup()

    @activity.defn
    async def preprocess(self, inputs: list[str]) -> list[list[PreprocessedInput]]:
        """Preprocess transcription inputs

        :param inputs: list of file paths
        :return: list of caul.tasks.preprocessing.helpers.PreprocessedInput
        """
        return self.asr_handler.preprocessor.process(inputs)

    @activity.defn
    async def infer(
        self, inputs: list[PreprocessedInput]
    ) -> list[ParakeetModelHandlerResult]:
        """Transcribe audio files.

        :param inputs: list of preprocessed inputs
        :return: list of inference handler results
        """
        # Load tensors
        for item in inputs:
            tensor, sample_rate = torchaudio.load(item.metadata.preprocessed_file_path)
            # normalize
            tensor = self.asr_handler.preprocessor.normalize(tensor, sample_rate)
            # assign
            item.tensor = tensor

        return self.asr_handler.inference_handler.process(inputs)

    @activity.defn
    async def postprocess(
        self, inputs: list[ParakeetModelHandlerResult]
    ) -> list[ParakeetModelHandlerResult]:
        """Postprocess and reorder transcriptions

        :param inputs: list of inference handler results
        :return: list of parakeet inference handler results
        """
        return self.asr_handler.postprocessor.process(inputs)
