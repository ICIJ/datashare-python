from pathlib import Path

TEST_PATH = Path(__file__).parent.parent
TEST_RESOURCES_PATH = TEST_PATH / "resources"
TEST_WAV_PATH = TEST_RESOURCES_PATH / "asr_test.wav"
TEST_MP4_PATH = TEST_RESOURCES_PATH / "asr_test.mp4"

PARAKEET_TEST_SEGMENT_START = 0.08

PARAKEET_TEST_SEGMENT_END = 1.26

TEST_WAV_TRANSCRIPTION = "To embrace the chaos that they fought in this battle."
TEST_MP4_TRANSCRIPTION = "This is a test."

PARAKEET_TEST_CONFIDENCE = -248

TEST_TIMESTAMPS = [{"start": 0, "end": 8000}]
