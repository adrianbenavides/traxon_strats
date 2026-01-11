class YoloStrategyError(Exception):
    """Base exception for YoloStrategy errors."""


class YoloWeightsEmptyError(YoloStrategyError):
    """Raised when YOLO weights are empty."""


class YoloNoApiDataError(YoloStrategyError):
    """Raised when no API data is available."""


class YoloApiDataNotUpToDateError(YoloStrategyError):
    """Raised when API data is not up-to-date."""
