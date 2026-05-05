import pytest
from io import BytesIO
from unittest.mock import AsyncMock
from src.application.use_cases.transcription.enrich_file_from_manifest import EnrichFileFromManifest
from src.application.dtos.request.transcription import ManifestEnrichmentRequestDTO
from src.domain.entities.call_processing_entity import CallProcessingEntity
from src.domain.entities.initiative_entity import InitiativeEntity, Storage
from src.domain.exceptions.ingestion_exceptions import BatchNotFound, InitiativeNotFound


def _dto(**kw):
    base = dict(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        file_name="audio.wav",
        xlsx_name="Copia_CSV.xlsx",
    )
    base.update(kw)
    return ManifestEnrichmentRequestDTO(**base)


def _batch():
    return CallProcessingEntity(
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        storage_container="c1",
    )


def _initiative():
    return InitiativeEntity(
        initiative="INIT",
        name="Initiative",
        storage=Storage(account_name="acc", account_key="key"),
    )


def _use_case(blob_storage, files_repo, call_repo, initiative_repo):
    return EnrichFileFromManifest(
        blob_storage_adapter=blob_storage,
        files_processing_repo=files_repo,
        call_processing_repo=call_repo,
        initiative_repo=initiative_repo,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_returns_early_when_xlsx_name_is_empty():
    blob_storage = AsyncMock()
    call_repo = AsyncMock()

    await _use_case(blob_storage, AsyncMock(), call_repo, AsyncMock()).execute(_dto(xlsx_name=""))

    call_repo.get_by_id.assert_not_awaited()
    blob_storage.download_file.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_batch_does_not_exist():
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = None

    with pytest.raises(BatchNotFound):
        await _use_case(AsyncMock(), AsyncMock(), call_repo, AsyncMock()).execute(_dto())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_initiative_does_not_exist():
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = None

    with pytest.raises(InitiativeNotFound):
        await _use_case(AsyncMock(), AsyncMock(), call_repo, initiative_repo).execute(_dto())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_returns_without_enriching_when_file_not_found_in_manifest(monkeypatch):
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    blob_storage = AsyncMock()
    blob_storage.download_file.return_value = BytesIO(b"")
    files_repo = AsyncMock()
    monkeypatch.setattr(
        "src.application.use_cases.transcription.enrich_file_from_manifest.parse_xlsx_manifest",
        lambda _: {},
    )
    monkeypatch.setattr(
        "src.application.use_cases.transcription.enrich_file_from_manifest.norm_wav_key",
        lambda _: "audio.wav",
    )

    await _use_case(blob_storage, files_repo, call_repo, initiative_repo).execute(_dto())

    files_repo.apply_xlsx_enrichment.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_applies_enrichment_when_file_found_in_manifest(monkeypatch):
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    blob_storage = AsyncMock()
    blob_storage.download_file.return_value = BytesIO(b"")
    files_repo = AsyncMock()
    row = {"Csv_Name": "audio.csv", "Conversation_ID": "cv1", "Consecutivo": "001"}
    monkeypatch.setattr(
        "src.application.use_cases.transcription.enrich_file_from_manifest.parse_xlsx_manifest",
        lambda _: {"audio.wav": row},
    )
    monkeypatch.setattr(
        "src.application.use_cases.transcription.enrich_file_from_manifest.norm_wav_key",
        lambda _: "audio.wav",
    )

    await _use_case(blob_storage, files_repo, call_repo, initiative_repo).execute(_dto())

    files_repo.apply_xlsx_enrichment.assert_awaited_once()
    assert files_repo.apply_xlsx_enrichment.await_args.args[0] == "f1"
