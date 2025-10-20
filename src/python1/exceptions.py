class DownloadException(Exception):
    pass

class NotFoundError(DownloadException):
    pass

class AccessDeniedError(DownloadException):
    pass