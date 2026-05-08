from enum import Enum

class RecordingFetchStatus(str, Enum):
    READY = "ready"
    NOT_READY = "not_ready"
    FAILED = "failed"

class ProcessingStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class RecordingStatus(str, Enum):
    PENDING = "pending"
    UPLOADED = "uploaded"
    FAILED = "failed"

class LLMStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"