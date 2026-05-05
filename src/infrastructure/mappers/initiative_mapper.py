from dataclasses import asdict
from typing import Any, Dict, Mapping, Optional
from src.domain.entities.initiative_entity import InitiativeEntity

_STORAGE_KEY_MAP = {
    "account_name": "accountName",
    "account_key": "accountKey",
    "container_input": "containerInput",
    "container_output": "containerOutput",
    "connection_string_key_vault_ref": "connectionStringKeyVaultRef",
}


def _to_legacy_storage_keys(storage: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if storage is None:
        return None
    return {
        _STORAGE_KEY_MAP.get(key, key): value
        for key, value in storage.items()
    }


def to_doc(entity: InitiativeEntity) -> Dict[str, Any]:
    doc = asdict(entity)
    doc["storage"] = _to_legacy_storage_keys(doc.get("storage"))
    doc["agents_storage"] = _to_legacy_storage_keys(doc.get("agents_storage"))
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
