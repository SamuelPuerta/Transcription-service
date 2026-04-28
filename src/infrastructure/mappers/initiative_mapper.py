from dataclasses import asdict
from typing import Any, Dict, Mapping, Optional
from src.domain.entities.initiative_entity import InitiativeEntity

def to_doc(entity: InitiativeEntity) -> Dict[str, Any]:
    doc = asdict(entity)
    return {k: v for k, v in doc.items() if v is not None}

def from_doc(d: Mapping[str, Any] | None) -> Optional[InitiativeEntity]:
    if not d:
        return None
    data: Dict[str, Any] = {
        "initiative": d.get("initiative", ""),
        "name": d.get("name", ""),
        "description": d.get("description"),
        "storage": d.get("storage"),
        "agent_storage": d.get("agent_storage"),
        "agents": d.get("agents"),
        "configuration": d.get("configuration"),
        "created_at": d.get("created_at"),
        "updated_at": d.get("updated_at"),
    }
    return InitiativeEntity(**data)