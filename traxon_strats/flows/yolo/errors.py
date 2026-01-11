class YoloFlowError(Exception):
    """Base exception for Yolo flow errors."""


class YoloNoApiDataError(YoloFlowError):
    """Raised when no API data is available."""


class YoloApiDataNotUpToDateError(YoloFlowError):
    """Raised when API data is not up-to-date."""
