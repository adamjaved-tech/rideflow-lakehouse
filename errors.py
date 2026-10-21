"""Exception hierarchy. Every pipeline failure derives from PipelineError so callers
can distinguish expected operational failures from programming errors."""


class PipelineError(Exception):
    """Base class for all expected pipeline failures."""


class ConfigError(PipelineError):
    """Configuration is missing or invalid."""


class SourceError(PipelineError):
    """The event source is unavailable or inconsistent with the checkpoint."""


class IngestError(PipelineError):
    """Bronze ingestion failed or the checkpoint is corrupt."""


class DataQualityError(PipelineError):
    """One or more critical data-quality checks failed."""

    def __init__(self, message: str, results=None):
        super().__init__(message)
        self.results = results or []


class WarehouseError(PipelineError):
    """Warehouse initialisation, migration or load failed."""
