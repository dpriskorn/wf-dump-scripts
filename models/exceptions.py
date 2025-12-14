class NoZidFound(Exception):
    """Raised when a ZID cannot be extracted from the data."""

    pass


class DateError(BaseException):
    """Raised when a date cannot be extracted"""

    pass


class MissingData(BaseException):
    pass


class NoTestResultFound(BaseException):
    pass
