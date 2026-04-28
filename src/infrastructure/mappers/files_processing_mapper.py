from dataclasses import asdict
from typing import Any, Dict, Mapping, Optional

from src.domain.entities.files_evaluation_enrichment_entity import (
    FilesEvaluationEnrichmentEntity,
)
from src.domain.entities.files_processing_entity import FilesProcessingEntity
from src.domain.entities.files_xlsx_enrichment_entity import FilesXlsxEnrichmentEntity


def to_doc(entity: FilesProcessingEntity) -> Dict[str, Any]:
    return _prune_none(asdict(entity))


def from_doc(d: Mapping[str, Any] | None) -> Optional[FilesProcessingEntity]:
    if not d:
        return None
    return FilesProcessingEntity.from_dict(dict(d))


def xlsx_enrichment_to_doc(entity: FilesXlsxEnrichmentEntity) -> Dict[str, Any]:
    return _prune_none(asdict(entity))


def evaluation_enrichment_to_doc(
    entity: FilesEvaluationEnrichmentEntity,
) -> Dict[str, Any]:
    return _prune_none(asdict(entity))


def _prune_none(value: Any):
    if isinstance(value, dict):
        return {k: _prune_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_prune_none(item) for item in value]
    return value
