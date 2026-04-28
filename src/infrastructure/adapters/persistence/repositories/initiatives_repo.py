from pymongo.asynchronous.collection import AsyncCollection
from src.infrastructure.mappers.initiative_mapper import from_doc, to_doc
from src.domain.entities.initiative_entity import InitiativeEntity
from src.domain.ports.infrastructure.persistence.initiatives import Initiatives
from src.infrastructure.adapters.persistence.repositories.mongo_generic_repo import MongoGenericRepo

class InitiativesRepo(MongoGenericRepo, Initiatives):
    _collection_name = "initiatives"
    _to_doc = staticmethod(to_doc)
    _from_doc = staticmethod(from_doc)
    _indexes = [
        (
            [("initiative", 1)],
            {
                "unique": True,
                "name": "uq_initiative",
            },
        ),
    ]

    def __init__(self, collection: AsyncCollection):
        super().__init__(collection)

    async def get_by_name(self, initiative: str) -> InitiativeEntity:
        document = await self.find_one({"initiative": initiative})
        return InitiativeEntity.from_dict(document) if document else None