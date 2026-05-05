from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    InvalidName,
    InvalidURI,
    NetworkTimeout,
    PyMongoError,
)
from src.config.settings import settings
from src.config.logger import logger
from src.infrastructure.exceptions.mongodb_exceptions import DatabaseConnectionError

def create_mongo_client() -> AsyncMongoClient:
    try:
        client = AsyncMongoClient(
            settings.mongo_connection_url,
            retryWrites=False,
            uuidRepresentation="standard",
        )
        return client
    except (InvalidURI, ConfigurationError) as e:
        logger.error("Configuracion de MongoDB invalida", context={"error": str(e)})
        raise DatabaseConnectionError(
            f"Configuración de conexión inválida: {e}", original_exception=e
        ) from e
    except (ConnectionFailure, NetworkTimeout) as e:
        logger.error("No se pudo conectar a MongoDB", context={"error": str(e)})
        raise DatabaseConnectionError(
            f"No se pudo establecer conexión con MongoDB: {e}", original_exception=e
        ) from e
    except PyMongoError as e:
        logger.error("Error inesperado al crear cliente MongoDB", context={"error": str(e)})
        raise DatabaseConnectionError(
            f"Error inesperado al crear cliente MongoDB: {e}", original_exception=e
        ) from e

def get_database(client: AsyncMongoClient, database_name: str) -> AsyncDatabase:
    try:
        return client.get_database(database_name)
    except (InvalidName, ConfigurationError) as e:
        logger.error("Nombre de base de datos invalido", context={"database": database_name, "error": str(e)})
        raise DatabaseConnectionError(
            f"Nombre de base de datos inválido '{database_name}': {e}", original_exception=e
        ) from e
    except PyMongoError as e:
        logger.error("Error al obtener base de datos", context={"database": database_name, "error": str(e)})
        raise DatabaseConnectionError(
            f"Error al obtener base de datos '{database_name}': {e}", original_exception=e
        ) from e
