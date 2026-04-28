from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return datetime.now(timezone.utc)

@dataclass
class Storage:
    accountName: str
    accountKey: str
    containerInput: Optional[str] = None
    containerOutput: Optional[str] = None
    connectionStringKeyVaultRef: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Storage":
        return Storage(
            accountName=data["accountName"],            
            accountKey=data["accountKey"],               
            containerInput=data.get("containerInput") or None,
            containerOutput=data.get("containerOutput") or None,
            connectionStringKeyVaultRef=data.get("connectionStringKeyVaultRef") or None,
        )

@dataclass
class Agent:
    id: str
    name: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Agent":
        return Agent(
            id=data["id"],    
            name=data["name"],   
        )

@dataclass
class Configuration:
    prompt: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Configuration":
        return Configuration(
            prompt=data["prompt"], 
        )

@dataclass
class InitiativeEntity:
    initiative: str
    name: str
    description: Optional[str] = None
    storage: Optional[Storage] = None
    agents_storage: Optional[Storage] = None
    agents: List[Agent] = field(default_factory=list)
    configuration: Optional[Configuration] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "InitiativeEntity":
        storage_data = data.get("storage")
        agents_storage_data = data.get("agents_storage")
        agents_data = data.get("agents") or []
        configuration_data = data.get("configuration")
        return InitiativeEntity(
            initiative=data["initiative"],  
            name=data["name"],               
            description=data.get("description") or None,
            storage=Storage.from_dict(storage_data) if isinstance(storage_data, dict) else None,
            agents_storage=Storage.from_dict(agents_storage_data) if isinstance(agents_storage_data, dict) else None,
            agents=[Agent.from_dict(agent) for agent in agents_data if isinstance(agent, dict)],
            configuration=Configuration.from_dict(configuration_data) if isinstance(configuration_data, dict) else None,
            created_at=_parse_datetime(data.get("created_at")),
            updated_at=_parse_datetime(data.get("updated_at")),
        )