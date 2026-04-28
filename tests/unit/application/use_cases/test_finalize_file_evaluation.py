import pytest
from unittest.mock import AsyncMock
from src.application.use_cases.evaluation.finalize_file_evaluation import FinalizeFileEvaluation
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.domain.entities.files_processing_entity import (
    FilesProcessingEntity,
    EvaluationResult,
    EvaluationMetadata,
)
from src.domain.entities.initiative_entity import InitiativeEntity, Configuration, Storage
from src.domain.exceptions.evaluation_exceptions import EvaluationDataIncomplete
from src.domain.exceptions.ingestion_exceptions import FileProcessingNotFound, InitiativeNotFound


def _dto(evaluation_json=None, **kw):
    base = dict(file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")
    base.update(kw)
    return ProcessEvaluationJobRequestDTO(**base, evaluation_json=evaluation_json)


def _file():
    return FilesProcessingEntity(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        file_name="audio.wav",
        blob_url="https://storage/audio.wav",
        evaluation_result=EvaluationResult(
            metadata=EvaluationMetadata(date_recording="2026-01-28", start_time="10:00:00"),
        ),
    )


def _initiative():
    return InitiativeEntity(
        initiative="INIT",
        name="Initiative",
        storage=Storage(accountName="acc", accountKey="key"),
        configuration=Configuration(prompt="Evaluate."),
    )


def _evaluation_json():
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


def _use_case(files_repo, call_repo, initiative_repo, complete):
    return FinalizeFileEvaluation(
        files_processing_repo=files_repo,
        call_processing_repo=call_repo,
        initiative_repo=initiative_repo,
        blob_storage_adapter=AsyncMock(),
        complete_batch_use_case=complete,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_evaluation_json_is_none():
    use_case = _use_case(AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(EvaluationDataIncomplete, match="evaluation_json"):
        await use_case.execute(_dto(evaluation_json=None))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_file_entity_does_not_exist():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = None
    use_case = _use_case(files_repo, AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(FileProcessingNotFound):
        await use_case.execute(_dto(evaluation_json={}))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_initiative_does_not_exist():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = None
    use_case = _use_case(files_repo, AsyncMock(), initiative_repo, AsyncMock())

    with pytest.raises(InitiativeNotFound):
        await use_case.execute(_dto(evaluation_json={}))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_applies_evaluation_enrichment_marks_completed_and_delegates_to_complete_batch():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    call_repo = AsyncMock()
    complete = AsyncMock()
    use_case = _use_case(files_repo, call_repo, initiative_repo, complete)

    await use_case.execute(_dto(evaluation_json=_evaluation_json()))

    files_repo.apply_evaluation_enrichment.assert_awaited_once()
    files_repo.mark_as_completed.assert_awaited_once_with("f1")
    call_repo.increment_completed.assert_awaited_once_with("INIT:2026-01-28", 1)
    complete.execute.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_preserves_existing_metadata_date_fields_when_applying_enrichment():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = FilesProcessingEntity(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        file_name="audio.wav",
        blob_url="https://storage/audio.wav",
        evaluation_result=EvaluationResult(
            metadata=EvaluationMetadata(
                date_recording="2026-01-28",
                start_time="10:00:00",
                end_time="10:10:00",
                duration_format="10:00",
            ),
        ),
    )
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    call_repo = AsyncMock()
    use_case = _use_case(files_repo, call_repo, initiative_repo, AsyncMock())

    await use_case.execute(_dto(evaluation_json=_evaluation_json()))

    enrichment_dto = files_repo.apply_evaluation_enrichment.await_args.args[1]
    assert enrichment_dto.evaluation_result.metadata.date_recording == "2026-01-28"
    assert enrichment_dto.evaluation_result.metadata.start_time == "10:00:00"
    assert enrichment_dto.evaluation_result.metadata.end_time == "10:10:00"
    assert enrichment_dto.evaluation_result.metadata.duration_format == "10:00"
    assert enrichment_dto.evaluation_result.metadata.cct_engineer == "CCT1"
    assert enrichment_dto.evaluation_result.metadata.xm_engineer == "XM1"
