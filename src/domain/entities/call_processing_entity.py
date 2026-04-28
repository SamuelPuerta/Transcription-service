from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from src.domain.value_objects.call_processing_status import CallProcessingStatus

def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return None

def _parse_call_status(value: Any) -> str:
    allowed = {
        CallProcessingStatus.COMPLETED,
        CallProcessingStatus.PENDING,
        CallProcessingStatus.FAILED,
        CallProcessingStatus.PROCESSING,
    }
    if isinstance(value, str):
        v = value.strip()
        return v if v in allowed else CallProcessingStatus.PENDING
    return CallProcessingStatus.PENDING

@dataclass
class CallProcessingEntity:
    batch_id: str
    initiative_id: str
    storage_container: str
    total_files: int = 0
    processed_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    status: str = field(default=CallProcessingStatus.PENDING)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def update_status(self, status: CallProcessingStatus):
        self.status = status
        self.updated_at = datetime.now(timezone.utc)

    def mark_as_started(self):
        if self.started_at is None:
            self.started_at = datetime.now(timezone.utc)
        if self.status == CallProcessingStatus.PENDING:
            self.update_status(CallProcessingStatus.PROCESSING)

    def mark_as_completed(self):
        if self.completed_at is None:
            self.completed_at = datetime.now(timezone.utc)
        self.update_status(CallProcessingStatus.COMPLETED)

    def increment_processed(self):
        self.processed_files += 1
        self.updated_at = datetime.now(timezone.utc)

    def increment_completed(self):
        self.completed_files += 1
        self.increment_processed()

    def increment_failed(self):
        self.failed_files += 1
        self.increment_processed()

    def increment_total_files(self):
        self.total_files += 1

    def check_completion(self):
        if self.processed_files >= self.total_files:
            if self.total_files > 0 and self.failed_files == self.total_files:
                self.update_status(CallProcessingStatus.FAILED)
            else:
                self.mark_as_completed()

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CallProcessingEntity":
        return CallProcessingEntity(
            batch_id=data["batch_id"],
            initiative_id=data["initiative_id"],
            storage_container=data["storage_container"],
            total_files=int(data.get("total_files") or 0),
            processed_files=int(data.get("processed_files") or 0),
            completed_files=int(data.get("completed_files") or 0),
            failed_files=int(data.get("failed_files") or 0),
            status=_parse_call_status(data.get("status")),
            created_at=_parse_datetime(data.get("created_at")) or datetime.now(timezone.utc),
            started_at=_parse_datetime(data.get("started_at")),
            completed_at=_parse_datetime(data.get("completed_at")),
            updated_at=_parse_datetime(data.get("updated_at")) or datetime.now(timezone.utc),
        )