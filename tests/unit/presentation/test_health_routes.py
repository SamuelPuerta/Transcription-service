import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import patch
from src.presentation.http.routes.health_routes import router


def _client():
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


@pytest.mark.unit
def test_health_endpoint_returns_200():
    with patch("src.presentation.http.routes.health_routes.settings") as mock_settings:
        mock_settings.app_name = "TranscriptionService"
        response = _client().get("/health")

    assert response.status_code == 200


@pytest.mark.unit
def test_health_endpoint_returns_ok_status_and_app_name():
    with patch("src.presentation.http.routes.health_routes.settings") as mock_settings:
        mock_settings.app_name = "TranscriptionService"
        response = _client().get("/health")

    body = response.json()
    assert body["status"] == "ok"
    assert body["app"] == "TranscriptionService"


@pytest.mark.unit
def test_healthz_endpoint_returns_200():
    with patch("src.presentation.http.routes.health_routes.settings") as mock_settings:
        mock_settings.app_name = "TranscriptionService"
        response = _client().get("/healthz")

    assert response.status_code == 200


@pytest.mark.unit
def test_healthz_endpoint_returns_same_body_as_health():
    with patch("src.presentation.http.routes.health_routes.settings") as mock_settings:
        mock_settings.app_name = "TranscriptionService"
        client = _client()
        health_body = client.get("/health").json()
        healthz_body = client.get("/healthz").json()

    assert health_body == healthz_body
