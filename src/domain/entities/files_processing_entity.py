from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from src.domain.value_objects.files_processing_status import FilesProcessingStatus

def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return None

@dataclass
class EvaluationMetadata:
    date_recording: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_format: Optional[str] = None
    cct_engineer: Optional[str] = None
    se_operator: Optional[str] = None
    xm_engineer: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "EvaluationMetadata":
        if not isinstance(data, dict):
            return None
        return EvaluationMetadata(      
            date_recording=data.get("date_recording") or None,        
            start_time=data.get("start_time") or None,                
            end_time=data.get("end_time") or None,                     
            duration_format=data.get("duration_format") or None,      
            cct_engineer=data.get("cct_engineer") or None,            
            se_operator=data.get("se_operator") or None,              
            xm_engineer=data.get("xm_engineer") or None,         
        )

@dataclass
class Evaluation:
    questions: List[Dict[str, Any]] = field(default_factory=list)
    total_points: Dict[str, Any] = field(default_factory=dict)
    average: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Evaluation":
        if not isinstance(data, dict):
            return None
        return Evaluation(
            questions=data.get("questions") or [],
            total_points=data.get("total_points") or {},
            average=data.get("average") or {},
        )

@dataclass
class EvaluationResult:
    metadata: Optional[EvaluationMetadata] = None
    evaluation: Optional[Evaluation] = None
    observations: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "EvaluationResult":
        if not isinstance(data, dict):
            return None
        metadata_data = data.get("metadata") or None
        evaluation_data = data.get("evaluation") or None
        return EvaluationResult(
            metadata=EvaluationMetadata.from_dict(metadata_data) if metadata_data else None,
            evaluation=Evaluation.from_dict(evaluation_data) if evaluation_data else None,
            observations=data.get("observations") or None,
        )

@dataclass
class OperativeEvent:
    date_occurrence: Optional[datetime] = None
    time_occurrence: Optional[str] = None
    herope_active: Optional[str] = None
    report_type: Optional[str] = None
    movement_type: Optional[str] = None
    designation_cause_e_logbook: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "OperativeEvent":
        if not isinstance(data, dict):
            return None
        return OperativeEvent(
            date_occurrence=_parse_datetime(data.get("date_occurrence")),
            time_occurrence=data.get("time_occurrence") or None,
            herope_active=data.get("herope_active") or None,
            report_type=data.get("report_type") or None,
            movement_type=data.get("movement_type") or None,
            designation_cause_e_logbook=data.get("designation_cause_e_logbook") or None
        )
    
@dataclass
class FilesProcessingEntity:
    batch_id: str
    file_name: str
    blob_url: str
    file_id: str
    xlsx_name: Optional[str] = None
    conversation_id: Optional[str] = None
    consecutive: Optional[str] = None
    csv_name: Optional[str] = None
    transcription: Optional[str] = None
    cct_engineer: Optional[str] = None
    se_operator: Optional[str] = None
    substation: Optional[str] = None
    engineer_score: Optional[float] = None
    operator_score: Optional[float] = None
    operative_event: Optional[OperativeEvent] = None
    evaluation_result: Optional[EvaluationResult] = None
    status: str = field(default=FilesProcessingStatus.PENDING)
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    processing_started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def update_status(self, status: FilesProcessingStatus):
        self.status = status
        if status == FilesProcessingStatus.PROCESSING and self.processing_started_at is None:
            self.processing_started_at = datetime.now(timezone.utc)
        elif status in [FilesProcessingStatus.COMPLETED, FilesProcessingStatus.FAILED]:
            if self.completed_at is None:
                self.completed_at = datetime.now(timezone.utc)

    def mark_as_processing(self):
        self.update_status(FilesProcessingStatus.PROCESSING)

    def mark_as_completed(self):
        self.update_status(FilesProcessingStatus.COMPLETED)

    def mark_as_failed(self):
        self.update_status(FilesProcessingStatus.FAILED)

    def set_transcription(self, transcription: str):
        self.transcription = transcription

    def set_operative_event(self, result: OperativeEvent):
        self.operative_event = result

    def set_evaluation_result(self, result: EvaluationResult):
        self.evaluation_result = result

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "FilesProcessingEntity":
        operative_event_data = data.get("operative_event")
        evaluation_result_data = data.get("evaluation_result")
        return FilesProcessingEntity(
            file_id=data["file_id"],
            batch_id=data["batch_id"],
            file_name=data["file_name"],
            blob_url=data["blob_url"],
            conversation_id=data.get("conversation_id") or None,
            consecutive=data.get("consecutive") or None,
            csv_name=data.get("csv_name") or None,
            xlsx_name=data.get("xlsx_name") or None,
            transcription=data.get("transcription") or None,
            cct_engineer=data.get("cct_engineer") or None,
            se_operator=data.get("se_operator") or None,
            substation=data.get("substation") or None,
            engineer_score=data.get("engineer_score") if data.get("engineer_score") is not None else None,
            operator_score=data.get("operator_score") if data.get("operator_score") is not None else None,
            error_message=data.get("error_message") or None,
            created_at=_parse_datetime(data.get("created_at")) or datetime.now(timezone.utc),
            processing_started_at=_parse_datetime(data.get("processing_started_at")),
            completed_at=_parse_datetime(data.get("completed_at")),
            updated_at=_parse_datetime(data.get("updated_at")) or datetime.now(timezone.utc),
            operative_event=OperativeEvent.from_dict(operative_event_data) if isinstance(operative_event_data, dict) else None,
            evaluation_result=EvaluationResult.from_dict(evaluation_result_data) if isinstance(evaluation_result_data, dict) else None,
            status=data.get("status"),
        )