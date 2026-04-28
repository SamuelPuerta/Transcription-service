import pytest
from unittest.mock import AsyncMock, MagicMock
from src.infrastructure.adapters.persistence.repositories.files_processing_repo import FilesProcessingRepo
from src.infrastructure.adapters.persistence.repositories.call_processing_repo import CallProcessingRepo
from src.infrastructure.adapters.persistence.repositories.initiatives_repo import InitiativesRepo
from src.domain.entities.files_processing_entity import FilesProcessingEntity, OperativeEvent, EvaluationResult
from src.domain.entities.call_processing_entity import CallProcessingEntity
from src.domain.entities.initiative_entity import InitiativeEntity
from src.domain.entities.files_xlsx_enrichment_entity import FilesXlsxEnrichmentEntity
from src.domain.entities.files_evaluation_enrichment_entity import FilesEvaluationEnrichmentEntity
from src.domain.value_objects.files_processing_status import FilesProcessingStatus
from src.domain.value_objects.call_processing_status import CallProcessingStatus


# ============================================================
# FilesProcessingRepo
# ============================================================

def _files_repo():
    collection = AsyncMock()
    collection.name = "files_processing"
    return FilesProcessingRepo(collection), collection


def _file():
    return FilesProcessingEntity(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        file_name="audio.wav",
        blob_url="https://storage/audio.wav",
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_get_by_id_returns_entity_when_found():
    repo, collection = _files_repo()
    collection.find_one.return_value = {
        "file_id": "f1", "batch_id": "B1", "file_name": "audio.wav",
        "blob_url": "https://storage/audio.wav", "status": "pending",
    }

    result = await repo.get_by_id("f1")

    assert isinstance(result, FilesProcessingEntity)
    assert result.file_id == "f1"
    collection.find_one.assert_awaited_once_with({"file_id": "f1"})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_get_by_id_returns_none_when_not_found():
    repo, collection = _files_repo()
    collection.find_one.return_value = None

    result = await repo.get_by_id("missing-id")

    assert result is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_create_returns_file_id_when_upserted():
    repo, collection = _files_repo()
    upsert_result = MagicMock()
    upsert_result.upserted_id = "some-mongo-id"
    collection.update_one.return_value = upsert_result

    result = await repo.create(_file())

    assert result == "f1"
    collection.update_one.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_create_returns_none_when_document_already_exists():
    repo, collection = _files_repo()
    upsert_result = MagicMock()
    upsert_result.upserted_id = None
    collection.update_one.return_value = upsert_result

    result = await repo.create(_file())

    assert result is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_mark_as_processing_sets_status_and_started_at():
    repo, collection = _files_repo()

    await repo.mark_as_processing("f1")

    collection.update_one.assert_awaited_once()
    call_args = collection.update_one.await_args
    query = call_args.args[0]
    update = call_args.args[1]
    assert query["file_id"] == "f1"
    assert update["$set"]["status"] == FilesProcessingStatus.PROCESSING
    assert "processing_started_at" in update["$set"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_set_transcription_sets_field_and_updated_at():
    repo, collection = _files_repo()

    await repo.set_transcription("f1", "transcribed text")

    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert update["$set"]["transcription"] == "transcribed text"
    assert "updated_at" in update["$set"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_mark_as_completed_sets_status_and_completed_at():
    repo, collection = _files_repo()

    await repo.mark_as_completed("f1")

    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert update["$set"]["status"] == FilesProcessingStatus.COMPLETED
    assert "completed_at" in update["$set"]
    assert update["$set"]["error_message"] is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_mark_as_failed_sets_status_error_message_and_completed_at():
    repo, collection = _files_repo()

    await repo.mark_as_failed("f1", "something went wrong")

    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert update["$set"]["status"] == FilesProcessingStatus.FAILED
    assert update["$set"]["error_message"] == "something went wrong"
    assert "completed_at" in update["$set"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_apply_xlsx_enrichment_calls_update_one_with_enrichment_doc():
    repo, collection = _files_repo()
    enrichment = FilesXlsxEnrichmentEntity(csv_name="a.csv", conversation_id="cv1", consecutive="001")

    await repo.apply_xlsx_enrichment("f1", enrichment)

    collection.update_one.assert_awaited_once()
    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert "updated_at" in update["$set"]
    assert update["$set"].get("csv_name") == "a.csv"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_apply_evaluation_enrichment_calls_update_one():
    repo, collection = _files_repo()
    enrichment = FilesEvaluationEnrichmentEntity(cct_engineer="CCT1", engineer_score=4.5)

    await repo.apply_evaluation_enrichment("f1", enrichment)

    collection.update_one.assert_awaited_once()
    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert "updated_at" in update["$set"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_get_by_blob_url_returns_entity_when_found():
    repo, collection = _files_repo()
    collection.find_one.return_value = {
        "file_id": "f1", "batch_id": "B1", "file_name": "audio.wav",
        "blob_url": "https://storage/audio.wav", "status": "pending",
    }

    result = await repo.get_by_blob_url("https://storage/audio.wav")

    assert isinstance(result, FilesProcessingEntity)
    collection.find_one.assert_awaited_once_with({"blob_url": "https://storage/audio.wav"})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_files_repo_get_by_blob_url_returns_none_when_not_found():
    repo, collection = _files_repo()
    collection.find_one.return_value = None

    result = await repo.get_by_blob_url("https://storage/missing.wav")

    assert result is None


# ============================================================
# CallProcessingRepo
# ============================================================

def _call_repo():
    collection = AsyncMock()
    collection.name = "call_processing"
    return CallProcessingRepo(collection), collection


def _batch():
    return CallProcessingEntity(
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        storage_container="c1",
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_get_by_id_returns_entity_when_found():
    repo, collection = _call_repo()
    collection.find_one.return_value = {
        "batch_id": "INIT:2026-01-28", "initiative_id": "INIT",
        "storage_container": "c1", "status": "processing",
    }

    result = await repo.get_by_id("INIT:2026-01-28")

    assert isinstance(result, CallProcessingEntity)
    assert result.batch_id == "INIT:2026-01-28"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_get_by_id_returns_none_when_not_found():
    repo, collection = _call_repo()
    collection.find_one.return_value = None

    result = await repo.get_by_id("missing")

    assert result is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_create_calls_update_one_with_upsert():
    repo, collection = _call_repo()
    collection.update_one.return_value = MagicMock()

    await repo.create(_batch())

    collection.update_one.assert_awaited_once()
    call_args = collection.update_one.await_args
    assert call_args.kwargs.get("upsert") is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_increment_total_files_increments_counter():
    repo, collection = _call_repo()

    await repo.increment_total_files("INIT:2026-01-28")

    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert update["$inc"]["total_files"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_increment_total_files_respects_custom_delta():
    repo, collection = _call_repo()

    await repo.increment_total_files("INIT:2026-01-28", delta=5)

    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert update["$inc"]["total_files"] == 5


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_increment_completed_increments_both_counters():
    repo, collection = _call_repo()

    await repo.increment_completed("INIT:2026-01-28", 1)

    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert update["$inc"]["completed_files"] == 1
    assert update["$inc"]["processed_files"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_increment_failed_increments_both_counters():
    repo, collection = _call_repo()

    await repo.increment_failed("INIT:2026-01-28", 1)

    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert update["$inc"]["failed_files"] == 1
    assert update["$inc"]["processed_files"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_check_completion_returns_status_from_document():
    repo, collection = _call_repo()
    collection.update_one.return_value = MagicMock()
    collection.find_one.return_value = {"status": CallProcessingStatus.COMPLETED}

    result = await repo.check_completion("INIT:2026-01-28")

    assert result == CallProcessingStatus.COMPLETED


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_check_completion_returns_pending_when_document_not_found():
    repo, collection = _call_repo()
    collection.update_one.return_value = MagicMock()
    collection.find_one.return_value = None

    result = await repo.check_completion("INIT:2026-01-28")

    assert result == CallProcessingStatus.PENDING


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_repo_mark_as_started_sets_started_at_and_processing_status():
    repo, collection = _call_repo()

    await repo.mark_as_started("INIT:2026-01-28")

    call_args = collection.update_one.await_args
    update = call_args.args[1]
    assert update["$set"]["status"] == CallProcessingStatus.PROCESSING
    assert "started_at" in update["$set"]


# ============================================================
# InitiativesRepo
# ============================================================

def _initiative_repo():
    collection = AsyncMock()
    collection.name = "initiatives"
    return InitiativesRepo(collection), collection


@pytest.mark.unit
@pytest.mark.asyncio
async def test_initiatives_repo_get_by_name_returns_entity_when_found():
    repo, collection = _initiative_repo()
    collection.find_one.return_value = {
        "initiative": "INIT", "name": "Initiative Test",
    }

    result = await repo.get_by_name("INIT")

    assert isinstance(result, InitiativeEntity)
    assert result.initiative == "INIT"
    collection.find_one.assert_awaited_once_with({"initiative": "INIT"})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_initiatives_repo_get_by_name_returns_none_when_not_found():
    repo, collection = _initiative_repo()
    collection.find_one.return_value = None

    result = await repo.get_by_name("MISSING")

    assert result is None
