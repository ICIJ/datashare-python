import uuid

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import (
    ErrorReportWithPages,
    ProcessedFile,
    ProcessingReportWithPages,
)
from extract_core import PipelineConfig
from extract_worker.objects import (
    MarkdownExtractArgs,
    MarkdownExtractResponse,
)
from extract_worker.workflows import ExtractMarkdownContentWorkflow, TaskQueues
from pydantic import TypeAdapter
from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker

c = """
{
        "pipeline": "docling",
        "device": "cpu",
        "format_options":
        {
            "pdf":
            {
                "pipeline_options":
                {
                    "document_timeout": null,
                    "accelerator_options":
                    {
                        "num_threads": 4,
                        "device": "cpu",
                        "cpu_use_flash_attention2": false
                    },
                    "enable_remote_services": false,
                    "allow_external_plugins": false,
                    "artifacts_path": null,
                    "do_picture_classification": false,
                    "picture_classification_options":
                    {
                        "kind": "document_picture_classifier",
                        "engine_options":
                        {
                            "engine_type": "transformers",
                            "top_k": null
                        },
                        "model_spec":
                        {
                            "name": "document_figure_classifier_v2",
                            "repo_id": "docling-project/DocumentFigureClassifier-v2.5",
                            "revision": "main",
                            "engine_overrides":
                            {}
                        }
                    },
                    "do_picture_description": false,
                    "picture_description_options":
                    {
                        "kind": "picture_description_vlm_engine",
                        "batch_size": 8,
                        "scale": 2.0,
                        "picture_area_threshold": 0.05,
                        "classification_allow": null,
                        "classification_deny": null,
                        "classification_min_confidence": 0.0,
                        "engine_options":
                        {
                            "engine_type": "auto_inline",
                            "prefer_vllm": false
                        },
                        "model_spec":
                        {
                            "name": "SmolVLM-256M-Instruct",
                            "default_repo_id": "HuggingFaceTB/SmolVLM-256M-Instruct",
                            "revision": "main",
                            "prompt": "Describe this image in a few sentences.",
                            "response_format": "plaintext",
                            "supported_engines": null,
                            "engine_overrides":
                            {
                                "mlx":
                                {
                                    "repo_id": "moot20/SmolVLM-256M-Instruct-MLX",
                                    "revision": null,
                                    "torch_dtype": null,
                                    "min_engine_version": null,
                                    "extra_config":
                                    {}
                                },
                                "transformers":
                                {
                                    "repo_id": null,
                                    "revision": null,
                                    "torch_dtype": "bfloat16",
                                    "min_engine_version": null,
                                    "extra_config":
                                    {
                                        "transformers_model_type": "automodel-imagetexttotext"
                                    }
                                }
                            },
                            "api_overrides":
                            {
                                "api_lmstudio":
                                {
                                    "params":
                                    {
                                        "model": "smolvlm-256m-instruct"
                                    }
                                }
                            },
                            "trust_remote_code": false,
                            "stop_strings":
                            [],
                            "temperature": 0.0,
                            "max_new_tokens": 4096,
                            "extra_generation_config":
                            {}
                        },
                        "prompt": "Describe this image in a few sentences.",
                        "generation_config":
                        {
                            "max_new_tokens": 200,
                            "do_sample": false
                        }
                    },
                    "do_chart_extraction": false,
                    "chart_extraction_options":
                    {
                        "kind": "chart_extraction_vlm_engine",
                        "engine_options":
                        {
                            "engine_type": "transformers",
                            "device": null,
                            "load_in_8bit": true,
                            "llm_int8_threshold": 6.0,
                            "quantized": false,
                            "torch_dtype": null,
                            "trust_remote_code": true,
                            "use_kv_cache": true,
                            "compile_model": false
                        },
                        "model_spec":
                        {
                            "name": "Granite-Vision-4.1-4B",
                            "default_repo_id": "ibm-granite/granite-vision-4.1-4b",
                            "revision": "dd48e97503de471803850df70843cf9eb5da8712",
                            "prompt": "<chart2csv>",
                            "response_format": "plaintext",
                            "supported_engines":
                            [
                                "api_ollama",
                                "transformers",
                                "api_lmstudio",
                                "api_openai"
                            ],
                            "engine_overrides":
                            {
                                "transformers":
                                {
                                    "repo_id": null,
                                    "revision": null,
                                    "torch_dtype": "bfloat16",
                                    "min_engine_version": null,
                                    "extra_config":
                                    {
                                        "transformers_model_type": "automodel-imagetexttotext"
                                    }
                                }
                            },
                            "api_overrides":
                            {
                                "api_lmstudio":
                                {
                                    "params":
                                    {
                                        "model": "granite-vision-4.1-4b"
                                    }
                                },
                                "api_ollama":
                                {
                                    "params":
                                    {
                                        "model": "granite-vision-4.1-4b"
                                    }
                                },
                                "api_openai":
                                {
                                    "params":
                                    {
                                        "model": "granite-vision-4.1-4b"
                                    }
                                }
                            },
                            "trust_remote_code": true,
                            "stop_strings":
                            [],
                            "temperature": 0.0,
                            "max_new_tokens": 4096,
                            "extra_generation_config":
                            {}
                        },
                        "chart2csv": true,
                        "chart2summary": false,
                        "chart2code": false,
                        "output_format": "granite_vision_charts",
                        "use_natural_language_prompts": false
                    },
                    "images_scale": 1.0,
                    "generate_page_images": false,
                    "generate_picture_images": true,
                    "do_table_structure": true,
                    "do_ocr": true,
                    "do_code_enrichment": false,
                    "do_formula_enrichment": false,
                    "force_backend_text": false,
                    "table_structure_options":
                    {
                        "kind": "docling_tableformer",
                        "do_cell_matching": true,
                        "mode": "accurate"
                    },
                    "ocr_options":
                    {
                        "kind": "rapidocr",
                        "mode": "full_page",
                        "lang":
                        [
                            "fr",
                            "en"
                        ],
                        "backend": "onnxruntime",
                        "scale": 3.0,
                        "force_full_page_ocr": false
                    },
                    "layout_options":
                    {
                        "kind": "layout_object_detection",
                        "keep_empty_clusters": false,
                        "skip_cell_assignment": false,
                        "create_orphan_clusters": true,
                        "engine_options":
                        {
                            "engine_type": "onnxruntime",
                            "score_threshold": 0.3
                        },
                        "model_spec":
                        {
                            "name": "layout_heron",
                            "repo_id": "docling-project/docling-layout-heron",
                            "revision": "main",
                            "engine_overrides":
                            {
                                "onnxruntime":
                                {
                                    "repo_id": "docling-project/docling-layout-heron-onnx",
                                    "revision": null,
                                    "torch_dtype": null,
                                    "min_engine_version": null,
                                    "extra_config":
                                    {
                                        "model_filename": "model.onnx"
                                    }
                                }
                            }
                        }
                    },
                    "code_formula_options":
                    {
                        "engine_options":
                        {
                            "engine_type": "auto_inline",
                            "prefer_vllm": false
                        },
                        "model_spec":
                        {
                            "name": "CodeFormulaV2",
                            "default_repo_id": "docling-project/CodeFormulaV2",
                            "revision": "main",
                            "prompt": "",
                            "response_format": "plaintext",
                            "supported_engines": null,
                            "engine_overrides":
                            {
                                "transformers":
                                {
                                    "repo_id": null,
                                    "revision": null,
                                    "torch_dtype": null,
                                    "min_engine_version": null,
                                    "extra_config":
                                    {
                                        "transformers_model_type": "automodel-imagetexttotext",
                                        "extra_generation_config":
                                        {
                                            "skip_special_tokens": false
                                        },
                                        "torch_dtype": "bfloat16"
                                    }
                                }
                            },
                            "api_overrides":
                            {},
                            "trust_remote_code": false,
                            "stop_strings":
                            [
                                "</doctag>",
                                "<end_of_utterance>"
                            ],
                            "temperature": 0.0,
                            "max_new_tokens": 4096,
                            "extra_generation_config":
                            {}
                        },
                        "scale": 2.0,
                        "max_size": null,
                        "extract_code": true,
                        "extract_formulas": true
                    },
                    "generate_table_images": false,
                    "generate_parsed_pages": false,
                    "heading_hierarchy_options":
                    {
                        "enabled": false,
                        "use_bookmarks": true,
                        "use_numbering": true,
                        "use_style": true,
                        "use_font_style": true,
                        "style_size_tolerance": 0.05,
                        "numbering_schemes": null,
                        "max_level": 6,
                        "bookmark_match_threshold": 0.8
                    },
                    "ocr_batch_size": 4,
                    "layout_batch_size": 4,
                    "table_batch_size": 4,
                    "batch_polling_interval_seconds": 0.5,
                    "queue_max_size": 100,
                    "stage_shutdown_timeout_seconds": 15.0
                },
                "backend": "ThreadedDoclingParseDocumentBackend",
                "backend_options": null,
                "pipeline_cls": "ThreadedStandardPdfPipeline"
            }
        }
    }
"""


@pytest.mark.e2e
async def test_extract_markdown_workflow_e2e(
    workflows_worker: Worker,  # noqa: ARG001
    io_worker: Worker,  # noqa: ARG001
    md_extract_cpu_worker: Worker,  # noqa: ARG001
    test_temporal_client: TemporalClient,
    docs_with_cached_artifacts: list[ProcessedFile],
) -> None:
    # Given
    client = test_temporal_client
    wf_id = f"extract-markdown-{uuid.uuid4()}"
    config = TypeAdapter(PipelineConfig).validate_json(c)
    doc_ids = [d.doc_id for d in docs_with_cached_artifacts]
    args = MarkdownExtractArgs(
        project=TEST_PROJECT,
        docs=doc_ids,
        config=config,
    )

    # When
    response = await client.execute_workflow(
        ExtractMarkdownContentWorkflow,
        args,
        id=wf_id,
        task_queue=TaskQueues.WORKFLOWS,
    )

    # Then
    response = response.model_dump()
    expected_res = MarkdownExtractResponse(
        processed=ProcessingReportWithPages(n_docs=2, n_pages=3),
        successes=ProcessingReportWithPages(n_docs=2, n_pages=3),
        errors=ErrorReportWithPages(n_docs=0, n_pages=0),
    ).model_dump()
    assert response == expected_res
