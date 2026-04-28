from fastapi import APIRouter
from src.config.settings import settings

router = APIRouter()

@router.get("/health", tags=["Health"])
@router.get("/healthz", tags=["Health"])
def healthcheck():
    return {"status": "ok", "app": settings.app_name}