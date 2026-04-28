# Ejecución en local

## Variables de Entorno Necesarias

```bash
SERVICE_BUS_NAMESPACE
SERVICE_BUS_STORAGE_EVENTS_QUEUE_NAME
SERVICE_BUS_TRANSCRIPTION_JOBS_QUEUE_NAME
SERVICE_BUS_EVALUATION_JOBS_QUEUE_NAME
SERVICE_BUS_STORAGE_EVENTS_CONNECTION_STRING
SERVICE_BUS_TRANSCRIPTION_JOBS_CONNECTION_STRING
SERVICE_BUS_EVALUATION_JOBS_CONNECTION_STRING
AZURE_OPENAI_ENDPOINT
AZURE_OPENAI_API_KEY
AZURE_OPENAI_API_VERSION
LLM_MODEL
MONGO_CONNECTION_URL
CATALOGO_DATABASE_NAME
INITIATIVE_COLLECTION
PROTOCOLO_DATABASE_NAME
FILES_PROCESSING_COLLECTION
CALL_PROCESSING_COLLECTION
AZURE_TENANT_ID
AZURE_CLIENT_ID
AZURE_CLIENT_SECRET
```

## En Sistemas Windows (con Uvicorn)
```bash
# Creación y activación de entorno virtual
python -m venv .venv
.\.venv\Scripts\activate

# Instalación de dependencias
pip install poetry
poetry lock
poetry install

# Ejecución con uvicorn
uvicorn src.presentation.main:app --reload
```

## En Sistemas Linux (con Gunicorn)

```bash
# Creación y activación de entorno virtual
python -m venv .venv
source .venv/bin/activate

# Instalación de dependencias
pip install poetry
poetry lock
poetry install

# Ejecución con gunicorn
gunicorn -c gunicorn.conf.py src.presentation.main:app
```

# Ejecución en local con Docker

```bash
# Creación del contenedor
docker build -t transcription-service .

# Activación del contenedor
docker run --name transcription-service-app -p 8000:8000 transcription-service

# Correr el Contenedor con una Instancia Nginx
docker compose up
```

# Endpoints de verificación

- **Healthcheck**: [http://localhost:8000/healthz](http://localhost:8000/healthz)  
- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs) 

# Ejecución de tests

```bash
# Test de Integración
pytest -m integration
# Test Unitarios
pytest -m unit
# Pruebas de Cobertura
coverage erase
coverage run -m pytest
# Exportar Resultados a XML y HTML
coverage html
coverage xml -o coverage.xml
# Exportar Resultados para Sonnar Qube
pytest --junitxml=xunit-reports\xunit-result.xml
```