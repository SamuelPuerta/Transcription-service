from dataclasses import asdict
from typing import Any, Dict, Mapping, Optional
from src.domain.entities.call_processing_entity import CallProcessingEntity

def to_doc(entity: CallProcessingEntity) -> Dict[str, Any]:
    doc = asdict(entity)
    return {k: v for k, v in doc.items() if v is not None}

def from_doc(d: Mapping[str, Any] | None) -> Optional[CallProcessingEntity]:
    if not d:
        return None
    return CallProcessingEntity.from_dict(dict(d))