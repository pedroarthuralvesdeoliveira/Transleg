class TranslegError(Exception):
    """Base exception for the Transleg pipeline."""


class ConfigurationError(TranslegError):
    """Raised when mandatory runtime configuration is missing."""


class ScrapingError(TranslegError):
    """Raised when the portal interaction fails."""


class ReportGenerationError(TranslegError):
    """Raised when the report monitor returns an unexpected state."""


class DataLoadError(TranslegError):
    """Raised when the warehouse write operation fails."""

