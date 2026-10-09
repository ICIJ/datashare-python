from collections.abc import Iterable
from itertools import groupby

from caul_core import ASRResult, SegmentIndex


def generic_unbatching_fn(batched_results: Iterable[ASRResult]) -> Iterable[ASRResult]:
    """Remap unordered and segmented tensors to original inputs for return

    :param batched_results: list of unordered results
    :return: list[ParakeetModelHandlerResult]
    """
    seen: set[int] = set()
    by_audio = groupby(batched_results, key=lambda r: r.index.audio)
    for audio_index, group in by_audio:
        seg_results = list(group)
        if audio_index in seen:
            msg = (
                f"expected contiguous segments, already processed segments from "
                f"audio of index {audio_index}"
            )
            raise ValueError(msg)
        # Drop segments with no recognized speech
        expected_indices = list(range(len(seg_results)))
        segment_indices = [r.index.segment for r in seg_results]
        if expected_indices != segment_indices:
            msg = (
                f"received audio segment results for audio {audio_index}"
                f" out of order: {segment_indices}"
            )
            raise ValueError(msg)
        seg_results = [r for r in seg_results if r.transcription]
        seg_results = sorted(seg_results, key=lambda r: r.transcription[0])

        seen.add(audio_index)
        base = ASRResult(
            index=SegmentIndex(audio=audio_index, segment=0),
            transcription=[],
            score=1.0,
        )
        merged_results = sum(seg_results, base)
        yield merged_results
