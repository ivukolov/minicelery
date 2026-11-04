


class MiniCeleryError(Exception):
    """Base class for exceptions"""

class MalformedMessageError(MiniCeleryError):
    """Exception raised when a message is malformed."""