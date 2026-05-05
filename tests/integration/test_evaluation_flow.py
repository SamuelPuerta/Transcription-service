import pytest
from unittest.mock import AsyncMock
from src.application.use_cases.evaluation.process_evaluation_job import (
    ProcessEvaluationJob,
    EVALUATION_USER_PROMPT_TEMPLATE,
)
from src.application.use_cases.evaluation.finalize_file_evaluation import FinalizeFileEvaluation
from src.application.use_cases.docs_gen.complete_batch import CompleteBatch
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.domain.entities.files_processing_entity import (
    FilesProcessingEntity,
    EvaluationResult,
    EvaluationMetadata,
)
from src.domain.entities.initiative_entity import InitiativeEntity, Configuration, Storage
from src.domain.entities.document_generation_job_entity import DocumentGenerationJobEntity
from src.domain.value_objects.files_processing_status import FilesProcessingStatus
from src.domain.value_objects.call_processing_status import CallProcessingStatus


def _dto(**kw):
    base = dict(file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")
    base.update(kw)
    return ProcessEvaluationJobRequestDTO(**base)


def _file(transcription="Hello world", consecutive="001"):
    return FilesProcessingEntity(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        file_name="audio.wav",
        blob_url="https://storage/audio.wav",
        status=FilesProcessingStatus.PENDING,
        transcription=transcription,
        consecutive=consecutive,
        evaluation_result=EvaluationResult(
            metadata=EvaluationMetadata(
                date_recording="2026-01-28",
                start_time="10:00:00",
                end_time="10:05:00",
                duration_format="05:00",
            )
        ),
    )


def _initiative():
    return InitiativeEntity(
        initiative="INIT",
        name="Initiative",
        storage=Storage(account_name="acc", account_key="key"),
        configuration=Configuration(prompt="Evaluate the transcript carefully."),
    )


def _ai_evaluation_json():
    return {
        "cct_engineer": "CCT1",
        "se_operator": "SE1",
        "substation": "SUB1",
        "engineer_score": 4.5,
        "operator_score": 3.0,
        "evaluation_result": {
            "metadata": {"cct_engineer": "CCT1", "se_operator": "SE1", "xm_engineer": "XM1"},
            "evaluation": {
                "questions": [{"id": 1, "score": 1}],
                "total_points": {"cct_engineer": 10},
                "average": {"cct_engineer": 1.0},
            },
            "observations": "Good call",
        },
    }


def _build_full_use_case(files_repo, call_repo, ai_gateway, initiative_repo, doc_gen_publisher):
    complete_batch = CompleteBatch(
        call_processing_repo=call_repo,
        document_generation_jobs_publisher=doc_gen_publisher,
    )
    finalize = FinalizeFileEvaluation(
        files_processing_repo=files_repo,
        call_processing_repo=call_repo,
        initiative_repo=initiative_repo,
        blob_storage_adapter=AsyncMock(),
        complete_batch_use_case=complete_batch,
    )
    return ProcessEvaluationJob(
        ai_gateway_adapter=ai_gateway,
        files_processing_repo=files_repo,
        call_processing_repo=call_repo,
        initiative_repo=initiative_repo,
        finalize_file_evaluation_use_case=finalize,
        complete_batch_use_case=complete_batch,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_evaluation_flow_completes_file_updates_batch_and_enqueues_document_generation(monkeypatch):
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    ai_gateway = AsyncMock()
    ai_gateway.chat_completion.return_value = '{"score": 1}'
    call_repo = AsyncMock()
    call_repo.check_completion.return_value = CallProcessingStatus.COMPLETED
    doc_gen_publisher = AsyncMock()
    monkeypatch.setattr(
        "src.application.use_cases.evaluation.process_evaluation_job.parse_json_str",
        lambda _: _ai_evaluation_json(),
    )

    use_case = _build_full_use_case(
        files_repo, call_repo, ai_gateway, initiative_repo, doc_gen_publisher
    )
    await use_case.execute(_dto())

    expected_prompt = EVALUATION_USER_PROMPT_TEMPLATE.format(transcription="Hello world", consecutive="001")
    ai_gateway.chat_completion.assert_awaited_once_with(
        system_prompt="Evaluate the transcript carefully.",
        user_prompt=expected_prompt,
    )
    files_repo.apply_evaluation_enrichment.assert_awaited_once()
    files_repo.mark_as_completed.assert_awaited_once_with("f1")
    call_repo.increment_completed.assert_awaited_once_with("INIT:2026-01-28", 1)

    assert doc_gen_publisher.enqueue.await_count >= 1
    enqueued: DocumentGenerationJobEntity = doc_gen_publisher.enqueue.await_args.args[0]
    assert enqueued.batch_id == "INIT:2026-01-28"
    assert enqueued.initiative_id == "INIT"

    files_repo.mark_as_failed.assert_not_awaited()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_evaluation_flow_does_not_enqueue_doc_generation_when_batch_still_has_files(monkeypatch):
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    ai_gateway = AsyncMock()
    ai_gateway.chat_completion.return_value = '{"score": 1}'
    call_repo = AsyncMock()
    call_repo.check_completion.return_value = CallProcessingStatus.PROCESSING
    doc_gen_publisher = AsyncMock()
    monkeypatch.setattr(
        "src.application.use_cases.evaluation.process_evaluation_job.parse_json_str",
        lambda _: _ai_evaluation_json(),
    )

    use_case = _build_full_use_case(
        files_repo, call_repo, ai_gateway, initiative_repo, doc_gen_publisher
    )
    await use_case.execute(_dto())

    files_repo.mark_as_completed.assert_awaited_once_with("f1")
    call_repo.increment_completed.assert_awaited_once_with("INIT:2026-01-28", 1)
    doc_gen_publisher.enqueue.assert_not_awaited()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_evaluation_flow_marks_failed_and_updates_batch_on_ai_gateway_error():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    ai_gateway = AsyncMock()
    ai_gateway.chat_completion.side_effect = RuntimeError("AI service unavailable")
    call_repo = AsyncMock()
    doc_gen_publisher = AsyncMock()

    use_case = _build_full_use_case(
        files_repo, call_repo, ai_gateway, initiative_repo, doc_gen_publisher
    )

    with pytest.raises(RuntimeError, match="AI service unavailable"):
        await use_case.execute(_dto())

    files_repo.mark_as_failed.assert_awaited_once_with("f1", "AI service unavailable")
    call_repo.increment_failed.assert_awaited_once_with("INIT:2026-01-28", 1)
    call_repo.check_completion.assert_awaited_once_with("INIT:2026-01-28")
    doc_gen_publisher.enqueue.assert_not_awaited()
    files_repo.mark_as_completed.assert_not_awaited()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_evaluation_flow_preserves_existing_metadata_when_finalizing(monkeypatch):
    files_repo = AsyncMock()
    file_with_metadata = _file()
    files_repo.get_by_id.return_value = file_with_metadata
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    ai_gateway = AsyncMock()
    ai_gateway.chat_completion.return_value = '{"score": 1}'
    call_repo = AsyncMock()
    call_repo.check_completion.return_value = CallProcessingStatus.COMPLETED
    doc_gen_publisher = AsyncMock()
    monkeypatch.setattr(
        "src.application.use_cases.evaluation.process_evaluation_job.parse_json_str",
        lambda _: _ai_evaluation_json(),
    )

    use_case = _build_full_use_case(
        files_repo, call_repo, ai_gateway, initiative_repo, doc_gen_publisher
    )
    await use_case.execute(_dto())

    enrichment_dto = files_repo.apply_evaluation_enrichment.await_args.args[1]
    assert enrichment_dto.evaluation_result.metadata.date_recording == "2026-01-28"
    assert enrichment_dto.evaluation_result.metadata.start_time == "10:00:00"
    assert enrichment_dto.evaluation_result.metadata.end_time == "10:05:00"
    assert enrichment_dto.evaluation_result.metadata.cct_engineer == "CCT1"
    assert enrichment_dto.evaluation_result.metadata.xm_engineer == "XM1"
