class SparkctlBaseException(Exception):
    """Base exception for all sparkctl exceptions"""


class InvalidConfiguration(SparkctlBaseException):
    """Raised when the input configuration is invalid."""


class OperationNotAllowed(SparkctlBaseException):
    """Raised when a user performs an invalid operation."""


class ExecutionError(SparkctlBaseException):
    """Raised when an external command fails."""
