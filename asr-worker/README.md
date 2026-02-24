# ASR Worker for Temporal

Module for starting an ASR worker process using Temporal (https://temporal.io)
task management. Transcription is performed by https://github.com/ICIJ/caul.
You'll need a Temporal server running somewhere to use the worker, which can
be passed to `asr_worker.worker.run_asr_worker` through the `target_host`
parameter.