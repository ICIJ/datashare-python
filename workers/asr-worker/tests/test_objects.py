import pytest
from asr_worker.objects import Transcript, Transcription

_TRANSCRIPTION_0 = Transcription(
    transcripts=[
        Transcript(text="speaker_0_sentence_0", speaker="speaker_0"),
    ],
    confidence=1.0,
)
_TRANSCRIPTION_1 = Transcription(
    transcripts=[
        Transcript(text="speaker_0_sentence_0", speaker="speaker_0"),
        Transcript(text="speaker_0_sentence_1", speaker="speaker_0"),
        Transcript(text="speaker_1_sentence_0", speaker="speaker_1"),
        Transcript(text="speaker_0_sentence_2", speaker="speaker_0"),
    ],
    confidence=1.0,
)


@pytest.mark.parametrize(
    ("transcript_sep", "speaker_sep", "transcription", "expected_text"),
    [
        ("\n", "\n\n", _TRANSCRIPTION_0, "speaker_0_sentence_0"),
        (
            "\n",
            "\n\n",
            _TRANSCRIPTION_1,
            """speaker_0_sentence_0
speaker_0_sentence_1

speaker_1_sentence_0

speaker_0_sentence_2""",
        ),
    ],
)
def test_asr_transcription_as_text(
    transcript_sep: str,
    speaker_sep: str,
    transcription: Transcription,
    expected_text: str,
) -> None:
    # When
    text = transcription.as_text(transcript_sep, speaker_sep=speaker_sep)
    # Then
    assert text == expected_text
