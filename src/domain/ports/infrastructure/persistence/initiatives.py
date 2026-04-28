from typing import Protocol
from src.domain.entities.initiative_entity import InitiativeEntity

class Initiatives(Protocol):
    async def get_by_name(self, initiative: str) -> InitiativeEntity: ...
