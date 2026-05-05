from fastapi import FastAPI
from pydantic import ValidationError
from src.domain.exceptions.base import DomainException
from src.infrastructure.exceptions.base import InfrastructureException
from src.config.logger import logger
from src.config.settings import settings
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from src.config.dependencies import build_infra_lifespan
from src.presentation.http.routes import health_routes
from .http.exception_handlers import(
    handle_domain_exception,
    handle_infrastructure_exception,
    handle_unhandled_exception,
    handle_validation_error
)

app = FastAPI(title=settings.app_name, lifespan=build_infra_lifespan())

app.add_exception_handler(DomainException, handle_domain_exception)
app.add_exception_handler(InfrastructureException, handle_infrastructure_exception)
app.add_exception_handler(ValidationError, handle_validation_error)
app.add_exception_handler(Exception, handle_unhandled_exception)

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, private"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response


app.add_middleware(SecurityHeadersMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_routes.router)

logger.info("Transcription Service iniciado", context={"app": settings.app_name})
