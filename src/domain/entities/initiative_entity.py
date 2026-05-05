from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return datetime.now(timezone.utc)

@dataclass(init=False)
class Storage:
    account_name: str
    account_key: str
    container_input: Optional[str] = None
    container_output: Optional[str] = None
    connection_string_key_vault_ref: Optional[str] = None

    def __init__(
        self,
        account_name: Optional[str] = None,
        account_key: Optional[str] = None,
        container_input: Optional[str] = None,
        container_output: Optional[str] = None,
        connection_string_key_vault_ref: Optional[str] = None,
        **legacy: Any,
    ) -> None:
        if account_name is None:
            account_name = legacy.pop("accountName", None)
        if account_key is None:
            account_key = legacy.pop("accountKey", None)
        if container_input is None:
            container_input = legacy.pop("containerInput", None)
        if container_output is None:
            container_output = legacy.pop("containerOutput", None)
        if connection_string_key_vault_ref is None:
            connection_string_key_vault_ref = legacy.pop("connectionStringKeyVaultRef", None)
        if legacy:
            unknown = ", ".join(sorted(legacy.keys()))
            raise TypeError(f"Unexpected Storage arguments: {unknown}")
        if account_name is None or account_key is None:
            raise TypeError("account_name and account_key are required")

        self.account_name = account_name
        self.account_key = account_key
        self.container_input = container_input or None
        self.container_output = container_output or None
        self.connection_string_key_vault_ref = connection_string_key_vault_ref or None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Storage":
        return Storage(
            account_name=data.get("account_name") or data["accountName"],
            account_key=data.get("account_key") or data["accountKey"],
            container_input=(data.get("container_input") if "container_input" in data else data.get("containerInput")) or None,
            container_output=(data.get("container_output") if "container_output" in data else data.get("containerOutput")) or None,
            connection_string_key_vault_ref=(
                data.get("connection_string_key_vault_ref")
                if "connection_string_key_vault_ref" in data
                else data.get("connectionStringKeyVaultRef")
            ) or None,
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
