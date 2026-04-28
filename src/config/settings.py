from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "Transcription Service"
    port: int = 8000
    mongo_connection_url: str
    catalogo_database_name: str
    protocolo_database_name: str
    initiative_collection: str
    files_processing_collection: str
    call_processing_collection: str
    service_bus_namespace: str
    service_bus_storage_events_queue_name: str
    service_bus_transcription_jobs_queue_name: str
    service_bus_evaluation_jobs_queue_name: str
    service_bus_document_generation_jobs_queue_name: str
    service_bus_storage_events_connection_string: str
    service_bus_transcription_jobs_connection_string: str
    service_bus_evaluation_jobs_connection_string: str
    service_bus_document_generation_jobs_connection_string: str
    ai_gateway_endpoint: str
    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()