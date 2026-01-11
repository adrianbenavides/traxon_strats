class RWApiError(Exception):
    """Base exception for RobotWealth API errors."""


class RwApiUnsuccessfulResponse(RWApiError):
    """Exception for unsuccessful API responses."""
