import pytest
from unittest.mock import AsyncMock, MagicMock
import httpx
from src.infrastructure.adapters.ai_gateway.ai_gateway_adapter import AIGatewayAdapter
from src.infrastructure.exceptions.ai_gateway_exceptions import (
    ExternalServiceError,
    ExternalTimeoutError,
    MappingError,
)
from src.domain.exceptions.transcription_exceptions import TranscriptionResultMissing


def _make_adapter():
    client = AsyncMock(spec=httpx.AsyncClient)
    adapter = AIGatewayAdapter(client=client)
    adapter._endpoint = "http://test-gateway"
    return adapter, client


def _mock_response(json_data, status_code=200):
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.json.return_value = json_data
    response.raise_for_status = MagicMock()
    return response


def _mock_http_status_error(status_code):
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    exc = httpx.HTTPStatusError("error", request=MagicMock(), response=response)
    return exc


# --- _extract_chat_completion_content ---

@pytest.mark.unit
def test_extract_chat_completion_content_returns_content_string():
    adapter, _ = _make_adapter()
    raw = {"choices": [{"message": {"content": "evaluation text"}}]}

    result = adapter._extract_chat_completion_content(raw)

    assert result == "evaluation text"


@pytest.mark.unit
def test_extract_chat_completion_content_raises_when_not_dict():
    adapter, _ = _make_adapter()

    with pytest.raises(MappingError, match="object"):
        adapter._extract_chat_completion_content(["not", "dict"])


@pytest.mark.unit
def test_extract_chat_completion_content_raises_when_choices_missing():
    adapter, _ = _make_adapter()

    with pytest.raises(MappingError, match="choices"):
        adapter._extract_chat_completion_content({})


@pytest.mark.unit
def test_extract_chat_completion_content_raises_when_choices_is_empty():
    adapter, _ = _make_adapter()

    with pytest.raises(MappingError, match="choices"):
        adapter._extract_chat_completion_content({"choices": []})


@pytest.mark.unit
def test_extract_chat_completion_content_raises_when_message_missing():
    adapter, _ = _make_adapter()

    with pytest.raises(MappingError, match="message"):
        adapter._extract_chat_completion_content({"choices": [{}]})


@pytest.mark.unit
def test_extract_chat_completion_content_raises_when_content_is_not_string():
    adapter, _ = _make_adapter()
    raw = {"choices": [{"message": {"content": 123}}]}

    with pytest.raises(MappingError, match="content"):
        adapter._extract_chat_completion_content(raw)


# --- _classify_and_raise ---

@pytest.mark.unit
def test_classify_and_raise_read_timeout_becomes_external_timeout_error():
    adapter, _ = _make_adapter()
    exc = httpx.ReadTimeout("timeout", request=MagicMock())

    with pytest.raises(ExternalTimeoutError):
        adapter._classify_and_raise(exc, operation="test")


@pytest.mark.unit
def test_classify_and_raise_connect_error_becomes_external_service_error():
    adapter, _ = _make_adapter()
    exc = httpx.ConnectError("refused")

    with pytest.raises(ExternalServiceError):
        adapter._classify_and_raise(exc, operation="test")


@pytest.mark.unit
def test_classify_and_raise_http_404_with_transcription_id_becomes_result_missing():
    adapter, _ = _make_adapter()
    exc = _mock_http_status_error(404)

    with pytest.raises(TranscriptionResultMissing):
        adapter._classify_and_raise(exc, operation="get_result", transcription_id="ts-1")


@pytest.mark.unit
def test_classify_and_raise_http_500_becomes_external_service_error():
    adapter, _ = _make_adapter()
    exc = _mock_http_status_error(500)

    with pytest.raises(ExternalServiceError):
        adapter._classify_and_raise(exc, operation="test")


@pytest.mark.unit
def test_classify_and_raise_key_error_becomes_mapping_error():
    adapter, _ = _make_adapter()

    with pytest.raises(MappingError):
        adapter._classify_and_raise(KeyError("missing"), operation="test")


@pytest.mark.unit
def test_classify_and_raise_value_error_becomes_mapping_error():
    adapter, _ = _make_adapter()

    with pytest.raises(MappingError):
        adapter._classify_and_raise(ValueError("bad format"), operation="test")


@pytest.mark.unit
def test_classify_and_raise_generic_exception_becomes_external_service_error():
    adapter, _ = _make_adapter()

    with pytest.raises(ExternalServiceError):
        adapter._classify_and_raise(RuntimeError("unknown"), operation="test")


# --- chat_completion ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_chat_completion_posts_to_chat_endpoint_and_returns_content():
    adapter, client = _make_adapter()
    client.post.return_value = _mock_response(
        {"choices": [{"message": {"content": "evaluation result"}}]}
    )

    result = await adapter.chat_completion("system prompt", "user prompt")

    assert result == "evaluation result"
    client.post.assert_awaited_once_with(
        "/chat",
        json=[
            {"role": "system", "content": "system prompt"},
            {"role": "user", "content": "user prompt"},
        ],
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_chat_completion_raises_external_timeout_on_read_timeout():
    adapter, client = _make_adapter()
    client.post.side_effect = httpx.ReadTimeout("timeout", request=MagicMock())

    with pytest.raises(ExternalTimeoutError):
        await adapter.chat_completion("sys", "usr")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_chat_completion_raises_mapping_error_on_invalid_response_structure():
    adapter, client = _make_adapter()
    client.post.return_value = _mock_response({"no_choices": True})

    with pytest.raises(MappingError):
        await adapter.chat_completion("sys", "usr")


# --- create_transcription ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_transcription_posts_and_returns_transcription_id():
    adapter, client = _make_adapter()
    client.post.return_value = _mock_response("ts-generated-123")

    result = await adapter.create_transcription("https://storage/audio.wav")

    assert result == "ts-generated-123"
    client.post.assert_awaited_once()
    call_args = client.post.await_args
    assert "audio_uri=https://storage/audio.wav" in call_args.args[0]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_transcription_raises_external_service_error_on_connect_error():
    adapter, client = _make_adapter()
    client.post.side_effect = httpx.ConnectError("connection refused")

    with pytest.raises(ExternalServiceError):
        await adapter.create_transcription("https://storage/audio.wav")


# --- get_transcription_status ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_transcription_status_returns_status_string():
    adapter, client = _make_adapter()
    client.get.return_value = _mock_response("Succeeded")

    result = await adapter.get_transcription_status("ts-1")

    assert result == "Succeeded"
    client.get.assert_awaited_once()
    assert "/transcription/status/ts-1" in client.get.await_args.args[0]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_transcription_status_404_raises_transcription_result_missing():
    adapter, client = _make_adapter()
    response_exc = _mock_http_status_error(404)
    client.get.side_effect = response_exc

    with pytest.raises((TranscriptionResultMissing, ExternalServiceError)):
        await adapter.get_transcription_status("ts-missing")


# --- get_transcription_result ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_transcription_result_returns_transcription_text():
    adapter, client = _make_adapter()
    client.get.return_value = _mock_response("The transcribed audio text")

    result = await adapter.get_transcription_result("ts-1")

    assert result == "The transcribed audio text"
    assert "/transcription/result/ts-1" in client.get.await_args.args[0]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_transcription_result_raises_external_timeout_on_read_timeout():
    adapter, client = _make_adapter()
    client.get.side_effect = httpx.ReadTimeout("timeout", request=MagicMock())

    with pytest.raises(ExternalTimeoutError):
        await adapter.get_transcription_result("ts-1")


# --- aclose ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_aclose_closes_http_client_and_sets_it_to_none():
    adapter, client = _make_adapter()

    await adapter.aclose()

    client.aclose.assert_awaited_once()
    assert adapter._client is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_aclose_is_idempotent_when_client_is_already_none():
    adapter, _ = _make_adapter()
    adapter._client = None

    await adapter.aclose()
