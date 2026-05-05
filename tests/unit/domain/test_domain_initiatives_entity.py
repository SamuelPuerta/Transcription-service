from datetime import datetime, timezone
import pytest
from src.domain.entities.initiative_entity import (
    Agent,
    Configuration,
    InitiativeEntity,
    Storage,
)

@pytest.mark.unit
def test_storage_from_dict_maps_optional_fields_to_none_when_empty():
    data = {
        "accountName": "acc",
        "accountKey": "key",
        "containerInput": "",
        "containerOutput": None,
        "connectionStringKeyVaultRef": "",
    }
    s = Storage.from_dict(data)
    assert s.account_name == "acc"
    assert s.account_key == "key"
    assert s.container_input is None
    assert s.container_output is None
    assert s.connection_string_key_vault_ref is None


@pytest.mark.unit
def test_storage_constructor_accepts_legacy_kwargs():
    s = Storage(accountName="acc", accountKey="key", containerInput="in")

    assert s.account_name == "acc"
    assert s.account_key == "key"
    assert s.container_input == "in"


@pytest.mark.unit
def test_storage_constructor_raises_for_unknown_kwargs():
    with pytest.raises(TypeError, match="Unexpected Storage arguments"):
        Storage(account_name="acc", account_key="key", unexpected="x")


@pytest.mark.unit
def test_storage_constructor_requires_required_fields():
    with pytest.raises(TypeError, match="account_name and account_key are required"):
        Storage()

@pytest.mark.unit
def test_agent_from_dict():
    a = Agent.from_dict({"id": "a1", "name": "Agent 1"})
    assert a.id == "a1"
    assert a.name == "Agent 1"

@pytest.mark.unit
def test_configuration_from_dict():
    c = Configuration.from_dict({"prompt": "hello"})
    assert c.prompt == "hello"

@pytest.mark.unit
def test_initiative_entity_from_dict_full_nested():
    now = datetime(2026, 1, 28, 12, 0, 0, tzinfo=timezone.utc)
    data = {
        "initiative": "init-1",
        "name": "Initiative",
        "description": "desc",
        "storage": {
            "accountName": "acc",
            "accountKey": "key",
            "containerInput": "in",
            "containerOutput": "out",
            "connectionStringKeyVaultRef": "kv-ref",
        },
        "agents_storage": {
            "accountName": "aacc",
            "accountKey": "akey",
            "containerInput": None,
            "containerOutput": "aout",
            "connectionStringKeyVaultRef": None,
        },
        "agents": [{"id": "ag1", "name": "Agent 1"}, {"id": "ag2", "name": "Agent 2"}],
        "configuration": {"prompt": "p"},
        "created_at": now,
        "updated_at": now,
    }
    e = InitiativeEntity.from_dict(data)
    assert e.initiative == "init-1"
    assert e.name == "Initiative"
    assert e.description == "desc"
    assert e.storage is not None
    assert e.storage.account_name == "acc"
    assert e.storage.container_input == "in"
    assert e.agents_storage is not None
    assert e.agents_storage.container_output == "aout"
    assert e.agents == [Agent(id="ag1", name="Agent 1"), Agent(id="ag2", name="Agent 2")]
    assert e.configuration is not None
    assert e.configuration.prompt == "p"
    assert e.created_at == now
    assert e.updated_at == now

@pytest.mark.unit
def test_initiative_entity_from_dict_missing_nested_defaults_agents_empty_list():
    now = datetime(2026, 1, 28, 12, 0, 0, tzinfo=timezone.utc)
    data = {
        "initiative": "init-1",
        "name": "Initiative",
        "created_at": now,
        "updated_at": now,
    }
    e = InitiativeEntity.from_dict(data)
    assert e.storage is None
    assert e.agents_storage is None
    assert e.configuration is None
    assert e.description is None
    assert e.agents == []


@pytest.mark.unit
def test_storage_from_dict_supports_snake_case_keys():
    data = {
        "account_name": "acc",
        "account_key": "key",
        "container_input": "in",
        "container_output": "out",
        "connection_string_key_vault_ref": "kv",
    }

    s = Storage.from_dict(data)

    assert s.account_name == "acc"
    assert s.account_key == "key"
    assert s.container_input == "in"
    assert s.container_output == "out"
    assert s.connection_string_key_vault_ref == "kv"
