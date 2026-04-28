import pytest
from src.domain.entities.blob_file_reference_entity import BlobFileReferenceEntity
from src.domain.entities.transcription_job_entity import TranscriptionJobEntity
from src.domain.entities.evaluation_job_entity import EvaluationJobEntity
from src.domain.entities.document_generation_job_entity import DocumentGenerationJobEntity
from src.domain.entities.files_xlsx_enrichment_entity import FilesXlsxEnrichmentEntity
from src.domain.entities.files_evaluation_enrichment_entity import FilesEvaluationEnrichmentEntity
from src.domain.entities.files_processing_entity import EvaluationResult, EvaluationMetadata, Evaluation, OperativeEvent


# --- BlobFileReferenceEntity ---

@pytest.mark.unit
def test_blob_file_reference_entity_stores_all_fields():
    entity = BlobFileReferenceEntity(
        account_name="myaccount",
        account_key="mykey",
        container_name="mycontainer",
        blob_path="INIT/2026-01-28/audio.wav",
    )

    assert entity.account_name == "myaccount"
    assert entity.account_key == "mykey"
    assert entity.container_name == "mycontainer"
    assert entity.blob_path == "INIT/2026-01-28/audio.wav"


@pytest.mark.unit
def test_blob_file_reference_entity_is_frozen():
    entity = BlobFileReferenceEntity("a", "b", "c", "d")

    with pytest.raises(Exception):
        entity.account_name = "changed"


# --- TranscriptionJobEntity ---

@pytest.mark.unit
def test_transcription_job_entity_stores_all_fields():
    entity = TranscriptionJobEntity(
        batch_id="INIT:2026-01-28",
        blob_url="https://storage/audio.wav",
        file_name="audio.wav",
        file_id="f1",
        initiative_id="INIT",
        storage_container="c1",
        transcription_id="ts-1",
        correlation_id="cid-1",
    )

    assert entity.batch_id == "INIT:2026-01-28"
    assert entity.blob_url == "https://storage/audio.wav"
    assert entity.file_name == "audio.wav"
    assert entity.file_id == "f1"
    assert entity.initiative_id == "INIT"
    assert entity.storage_container == "c1"
    assert entity.transcription_id == "ts-1"


@pytest.mark.unit
def test_transcription_job_entity_is_frozen():
    entity = TranscriptionJobEntity("b", "u", "fn", "fi", "ii", "sc", "ti", "cid-1")

    with pytest.raises(Exception):
        entity.file_id = "changed"


# --- EvaluationJobEntity ---

@pytest.mark.unit
def test_evaluation_job_entity_stores_all_fields():
    entity = EvaluationJobEntity(batch_id="INIT:2026-01-28", file_id="f1", initiative_id="INIT", correlation_id="cid-1")

    assert entity.batch_id == "INIT:2026-01-28"
    assert entity.file_id == "f1"
    assert entity.initiative_id == "INIT"


@pytest.mark.unit
def test_evaluation_job_entity_is_frozen():
    entity = EvaluationJobEntity("b", "f", "i", "cid-1")

    with pytest.raises(Exception):
        entity.batch_id = "changed"


# --- DocumentGenerationJobEntity ---

@pytest.mark.unit
def test_document_generation_job_entity_stores_all_fields():
    entity = DocumentGenerationJobEntity(batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")

    assert entity.batch_id == "INIT:2026-01-28"
    assert entity.initiative_id == "INIT"


@pytest.mark.unit
def test_document_generation_job_entity_is_frozen():
    entity = DocumentGenerationJobEntity("b", "i", "cid-1")

    with pytest.raises(Exception):
        entity.initiative_id = "changed"


# --- FilesXlsxEnrichmentEntity ---

@pytest.mark.unit
def test_files_xlsx_enrichment_entity_defaults_all_fields_to_none():
    entity = FilesXlsxEnrichmentEntity()

    assert entity.csv_name is None
    assert entity.xlsx_name is None
    assert entity.conversation_id is None
    assert entity.consecutive is None
    assert entity.operative_event is None
    assert entity.evaluation_result is None


@pytest.mark.unit
def test_files_xlsx_enrichment_entity_stores_provided_values():
    result = EvaluationResult()
    event = OperativeEvent()
    entity = FilesXlsxEnrichmentEntity(
        csv_name="a.csv",
        xlsx_name="a.xlsx",
        conversation_id="cv1",
        consecutive="001",
        operative_event=event,
        evaluation_result=result,
    )

    assert entity.csv_name == "a.csv"
    assert entity.xlsx_name == "a.xlsx"
    assert entity.conversation_id == "cv1"
    assert entity.consecutive == "001"
    assert entity.operative_event is event
    assert entity.evaluation_result is result


# --- FilesEvaluationEnrichmentEntity ---

@pytest.mark.unit
def test_files_evaluation_enrichment_entity_defaults_scores_to_zero():
    entity = FilesEvaluationEnrichmentEntity()

    assert entity.engineer_score == 0.0
    assert entity.operator_score == 0.0
    assert entity.cct_engineer is None
    assert entity.se_operator is None
    assert entity.substation is None
    assert entity.evaluation_result is None


@pytest.mark.unit
def test_files_evaluation_enrichment_entity_stores_provided_values():
    result = EvaluationResult()
    entity = FilesEvaluationEnrichmentEntity(
        cct_engineer="CCT1",
        se_operator="SE1",
        substation="SUB1",
        engineer_score=4.5,
        operator_score=3.0,
        evaluation_result=result,
    )

    assert entity.cct_engineer == "CCT1"
    assert entity.se_operator == "SE1"
    assert entity.substation == "SUB1"
    assert entity.engineer_score == 4.5
    assert entity.operator_score == 3.0
    assert entity.evaluation_result is result
