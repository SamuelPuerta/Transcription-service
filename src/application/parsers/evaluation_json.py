import json
from src.domain.exceptions.evaluation_exceptions import EvaluationDataIncomplete

def parse_json_str(value: str) -> dict:
        if value is None:
            raise EvaluationDataIncomplete("evaluation is None")
        stripped = value.strip()
        if not stripped:
            raise EvaluationDataIncomplete("evaluation is empty/whitespace")
        if stripped.startswith("```"):
            stripped = stripped.strip("`")
            stripped = stripped.replace("json\n", "", 1).strip()
        return json.loads(stripped)