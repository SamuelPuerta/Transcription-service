import pytest
from datetime import datetime, timezone
from src.infrastructure.mappers.files_processing_mapper import (
    to_doc,
    from_doc,
    xlsx_enrichment_to_doc,
    evaluation_enrichment_to_doc,
)
from src.infrastructure.mappers.call_processing_mapper import (
    to_doc as call_to_doc,
    from_doc as call_from_doc,
)
from src.infrastructure.mappers.initiative_mapper import (
    to_doc as initiative_to_doc,
    from_doc as initiative_from_doc,
)
from src.domain.entities.files_processing_entity import FilesProcessingEntity, EvaluationResult, EvaluationMetadata
from src.domain.entities.call_processing_entity import CallProcessingEntity
from src.domain.entities.initiative_entity import InitiativeEntity, Storage, Configuration
from src.domain.entities.files_xlsx_enrichment_entity import FilesXlsxEnrichmentEntity
from src.domain.entities.files_evaluation_enrichment_entity import FilesEvaluationEnrichmentEntity


def _now():
    return datetime(2026, 1, 28, 12, 0, 0, tzinfo=timezone.utc)


# ============================================================
# files_processing_mapper
# ============================================================

@pytest.mark.unit
def test_files_to_doc_converts_entity_to_dict():
    entity = FilesProcessingEntity(
        file_id="f1", batch_id="INIT:2026-01-28",
        file_name="audio.wav", blob_url="https://storage/audio.wav",
    )

    doc = to_doc(entity)

    assert doc["file_id"] == "f1"
    assert doc["batch_id"] == "INIT:2026-01-28"
    assert doc["file_name"] == "audio.wav"


@pytest.mark.unit
def test_files_to_doc_prunes_none_values():
    entity = FilesProcessingEntity(
        file_id="f1", batch_id="B1",
        file_name="audio.wav", blob_url="https://storage/audio.wav",
        transcription=None,
    )

    doc = to_doc(entity)

    assert "transcription" not in doc


@pytest.mark.unit
def test_files_from_doc_returns_entity_when_doc_is_provided():
    doc = {
        "file_id": "f1", "batch_id": "B1",
        "file_name": "audio.wav", "blob_url": "https://storage/audio.wav",
        "status": "pending",
    }

    entity = from_doc(doc)

    assert isinstance(entity, FilesProcessingEntity)
    assert entity.file_id == "f1"


@pytest.mark.unit
def test_files_from_doc_returns_none_when_doc_is_none():
    assert from_doc(None) is None


@pytest.mark.unit
def test_files_from_doc_returns_none_when_doc_is_empty():
    assert from_doc({}) is None


@pytest.mark.unit
def test_xlsx_enrichment_to_doc_includes_non_none_fields():
    enrichment = FilesXlsxEnrichmentEntity(
        csv_name="a.csv", conversation_id="cv1", consecutive="001"
    )

    doc = xlsx_enrichment_to_doc(enrichment)

    assert doc["csv_name"] == "a.csv"
    assert doc["conversation_id"] == "cv1"
    assert doc["consecutive"] == "001"
    assert "xlsx_name" not in doc


@pytest.mark.unit
def test_evaluation_enrichment_to_doc_includes_non_none_fields():
    enrichment = FilesEvaluationEnrichmentEntity(
        cct_engineer="CCT1",
        engineer_score=4.5,
        operator_score=0.0,
    )

    doc = evaluation_enrichment_to_doc(enrichment)

    assert doc["cct_engineer"] == "CCT1"
    assert doc["engineer_score"] == 4.5


# ============================================================
# call_processing_mapper
# ============================================================

@pytest.mark.unit
def test_call_to_doc_converts_entity_to_dict():
    entity = CallProcessingEntity(
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        storage_container="c1",
    )

    doc = call_to_doc(entity)

    assert doc["batch_id"] == "INIT:2026-01-28"
    assert doc["initiative_id"] == "INIT"
    assert doc["storage_container"] == "c1"


@pytest.mark.unit
def test_call_to_doc_prunes_none_values():
    entity = CallProcessingEntity(
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        storage_container="c1",
        started_at=None,
    )

    doc = call_to_doc(entity)

    assert "started_at" not in doc


@pytest.mark.unit
def test_call_from_doc_returns_entity_when_doc_is_provided():
    doc = {
        "batch_id": "INIT:2026-01-28",
        "initiative_id": "INIT",
        "storage_container": "c1",
        "status": "processing",
    }

    entity = call_from_doc(doc)

    assert isinstance(entity, CallProcessingEntity)
    assert entity.batch_id == "INIT:2026-01-28"


@pytest.mark.unit
def test_call_from_doc_returns_none_when_doc_is_none():
    assert call_from_doc(None) is None


# ============================================================
# initiative_mapper
# ============================================================

@pytest.mark.unit
def test_initiative_to_doc_converts_entity_to_dict():
    entity = InitiativeEntity(
        initiative="INIT",
        name="Test Initiative",
    )

    doc = initiative_to_doc(entity)

    assert doc["initiative"] == "INIT"
    assert doc["name"] == "Test Initiative"


@pytest.mark.unit
def test_initiative_from_doc_returns_none_when_doc_is_none():
    assert initiative_from_doc(None) is None


@pytest.mark.unit
def test_initiative_from_doc_returns_none_when_doc_is_empty():
    assert initiative_from_doc({}) is None
