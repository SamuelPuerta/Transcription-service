import json
import pytest
from src.application.parsers.evaluation_json import parse_json_str
from src.domain.exceptions.evaluation_exceptions import EvaluationDataIncomplete


@pytest.mark.unit
def test_raises_when_value_is_none():
    with pytest.raises(EvaluationDataIncomplete, match="None"):
        parse_json_str(None)


@pytest.mark.unit
def test_raises_when_value_is_empty_string():
    with pytest.raises(EvaluationDataIncomplete, match="empty"):
        parse_json_str("")


@pytest.mark.unit
def test_raises_when_value_is_whitespace_only():
    with pytest.raises(EvaluationDataIncomplete, match="empty"):
        parse_json_str("   ")


@pytest.mark.unit
def test_parses_plain_json_object():
    result = parse_json_str('{"key": "value", "num": 42}')

    assert result == {"key": "value", "num": 42}


@pytest.mark.unit
def test_parses_fenced_json_with_language_tag():
    result = parse_json_str('```json\n{"key": "value"}\n```')

    assert result == {"key": "value"}


@pytest.mark.unit
def test_parses_fenced_json_without_language_tag():
    result = parse_json_str('```\n{"key": "value"}\n```')

    assert result == {"key": "value"}


@pytest.mark.unit
def test_raises_on_invalid_json_syntax():
    with pytest.raises(json.JSONDecodeError):
        parse_json_str("{invalid json}")
