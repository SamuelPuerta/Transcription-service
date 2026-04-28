from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import errors
from src.infrastructure.exceptions.mongodb_exceptions import DatabaseOperationError

CALL_PROCESSING_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "batch_id", "initiative_id", "total_files", "status",
            "created_at", "updated_at"
        ],
        "properties": {
            "batch_id": {"bsonType": "string"},  
            "initiative_id": {"bsonType": "string"},
            "total_files":  {"bsonType": "int"},
            "processed_files":  {"bsonType": "int"},
            "completed_files":{"bsonType": "int"},
            "failed_files":{"bsonType": "int"},
            "status": {"bsonType": "string"},
            "storage_container": {"bsonType": "string"},
            "created_at": {"bsonType": "date"},
            "started_at": {"bsonType": ["date","null"]},
            "completed_at": {"bsonType": ["date","null"]},
            "updated_at": {"bsonType": "date"},
        }
    }
}

FILES_PROCESSING_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "file_id", "batch_id", "file_name",
            "status", "created_at", "updated_at"
        ],
        "properties": {
            "file_id": {"bsonType": "string"},  # Storage
            "batch_id": {"bsonType": "string"},  # Storage
            "conversation_id": {"bsonType": ["string","null"]}, # Traducción
            "consecutive": {"bsonType": ["string","null"]},  # Traducción
            "file_name": {"bsonType": "string"},  # Storage
            "csv_name": {"bsonType": ["string","null"]}, # Traducción
            "xlsx_name": {"bsonType": ["string","null"]},  # Traducción
            "blob_url": {"bsonType": "string"},  # Storage
            "transcription": {"bsonType": ["string","null"]}, # Evaluación
            "cct_engineer": {"bsonType": ["string","null"]}, # Evaluación
            "se_operator": {"bsonType": ["string","null"]}, # Evaluación
            "substation": {"bsonType": ["string","null"]}, # Evaluación
            "engineer_score": {"bsonType": ["double","null"]}, # Evaluación
            "operator_score": {"bsonType": ["double","null"]}, # Evaluación
            "operative_event": {
                "bsonType": ["object", "null"],
                "properties": {
                    "date_occurrence": {"bsonType": "date"},  # Traducción
                    "time_occurrence": {"bsonType": "string"},  # Traducción
                    "herope_active": {"bsonType": "string"},  # Traducción
                    "report_type": {"bsonType": "string"},  # Traducción
                    "movement_type": {"bsonType": "string"},  # Traducción
                    "designation_cause_e_logbook": {"bsonType": "string"},  # Traducción
                }
            },
            "evaluation_result": {
                "bsonType": ["object", "null"],
                "properties": {
                    "metadata": {
                        "bsonType": ["object", "null"],
                        "properties": {
                            "date_recording": {"bsonType": ["string", "null"]},  # Traducción
                            "start_time": {"bsonType": ["string", "null"]},  # Traducción
                            "end_time": {"bsonType": ["string", "null"]},  # Traducción
                            "duration_format": {"bsonType": ["string", "null"]},  # Traducción
                            "cct_engineer": {"bsonType": ["string", "null"]}, # Evaluación
                            "se_operator": {"bsonType": ["string", "null"]}, # Evaluación
                            "xm_engineer": {"bsonType": ["string", "null"]}, # Evaluación
                        }
                    },
                    "evaluation": {
                        "bsonType": ["object", "null"],
                        "properties": {
                            "questions": {"bsonType": ["array", "null"]}, # Evaluación
                            "total_points": {"bsonType": ["object", "null"]}, # Evaluación
                            "average": {"bsonType": ["object", "null"]}  # Evaluación
                        }
                    },
                    "observations": {"bsonType": ["string", "null"]} # Evaluación
                }
            },
            "status": {"bsonType": "string"},
            "error_message": {"bsonType": ["string", "null"]},
            "created_at": {"bsonType": "date"},
            "processing_started_at": {"bsonType": ["date","null"]},
            "completed_at": {"bsonType": ["date","null"]},
            "updated_at": {"bsonType": "date"}
        }
    }
}

async def ensure_collection_with_validator(
    db: AsyncIOMotorDatabase,
    name: str,
    validator: dict
) -> None:
    try:
        existing = await db.list_collection_names()
        if name not in existing:
            await db.create_collection(
                name,
                validator=validator
            )
    except errors.CollectionInvalid:
        pass  
    except Exception as e:
        raise DatabaseOperationError(f"Error ensuring {name} collection: {e}")

async def ensure_call_processing_collection(db: AsyncIOMotorDatabase):
    await ensure_collection_with_validator(
        db,
        "call_processing",
        CALL_PROCESSING_VALIDATOR
    )   
    
async def ensure_files_processing_collection(db: AsyncIOMotorDatabase):
    await ensure_collection_with_validator(
        db,
        "files_processing",
        FILES_PROCESSING_VALIDATOR
    )