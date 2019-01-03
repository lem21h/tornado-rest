# coding=utf-8
from http import HTTPStatus
from typing import NamedTuple

import tornado.web


class ErrorEntry(NamedTuple):
    status: int
    message: str


class BasicErrorCodes(object):
    GENERAL_NOT_FOUND = ErrorEntry(4000, 'Endpoint does not exists')
    METHOD_NOT_IMPLEMENTED = ErrorEntry(4001, 'Method not implemented yet')
    METHOD_NOT_SUPPORTED = ErrorEntry(4002, 'Method not supported')
    CANNOT_PERFORM_THIS_ACTION = ErrorEntry(4003, 'Cannot perform this action')
    INVALID_CONTENT = ErrorEntry(4004, 'Request has invalid content')

    UNDEFINED_ERROR = ErrorEntry(4005, 'An unexpected error has occurred')
    REQUIRES_AUTHORIZATION = ErrorEntry(4006, 'Authorization required')
    AUTHORIZATION_DATA_MISSING = ErrorEntry(4007, 'Missing authorization data')

    MISSING_REQUEST_DATA = ErrorEntry(4002, 'Missing data in requests')
    VALIDATION_ERROR = ErrorEntry(4008, 'Request validation error')
    BAD_UUID = ErrorEntry(4009, 'Badly formed uuid')
    EMAIL_NOT_VALID = ErrorEntry(4010, 'Provided email is not valid')
    EMAIL_REGISTERED = ErrorEntry(4011, 'Email address already taken')

    STORE_TO_DATABASE = ErrorEntry(4012, 'Error storing result in database')


class HTTPBaseError(tornado.web.HTTPError):
    __slots__ = ('code', 'message', 'details', 'status_code', 'args', 'reason', 'log_message')

    def __init__(self,
                 response: ErrorEntry,
                 status_code: int = 500,
                 log_message: str = None,
                 response_details: dict = None,
                 *args, **kwargs):
        super(HTTPBaseError, self).__init__(status_code, log_message, *args, **kwargs)

        self.details = response_details
        if isinstance(response, ErrorEntry):
            self.code = response.status
            self.message = response.message
        else:
            self.code = 0
            self.message = BasicErrorCodes.UNDEFINED_ERROR


class HTTP400BadRequestError(HTTPBaseError):
    def __init__(self, response: ErrorEntry, details=None):
        HTTPBaseError.__init__(
            self,
            response,
            HTTPStatus.BAD_REQUEST.value,
            HTTPStatus.BAD_REQUEST.name,
            details
        )


class HTTP401UnauthorizedError(HTTPBaseError):
    def __init__(self, response: ErrorEntry, details=None):
        HTTPBaseError.__init__(
            self,
            response,
            HTTPStatus.UNAUTHORIZED.value,
            HTTPStatus.UNAUTHORIZED.name,
            details
        )


class HTTP403ForbiddenError(HTTPBaseError):
    def __init__(self, response: ErrorEntry, details=None):
        HTTPBaseError.__init__(
            self,
            response,
            HTTPStatus.FORBIDDEN.value,
            HTTPStatus.FORBIDDEN.name,
            details
        )


class HTTP404NotFoundError(HTTPBaseError):
    def __init__(self, response: ErrorEntry, details=None):
        HTTPBaseError.__init__(
            self,
            response,
            HTTPStatus.NOT_FOUND.value,
            HTTPStatus.NOT_FOUND.name,
            details
        )


class HTTP405MethodNotAllowed(HTTPBaseError):
    def __init__(self, response: ErrorEntry, details=None):
        HTTPBaseError.__init__(
            self,
            response,
            HTTPStatus.METHOD_NOT_ALLOWED.value,
            HTTPStatus.METHOD_NOT_ALLOWED.name,
            details
        )


class HTTP406NotAcceptable(HTTPBaseError):
    def __init__(self, response: ErrorEntry, details=None):
        HTTPBaseError.__init__(
            self,
            response,
            HTTPStatus.NOT_ACCEPTABLE.value,
            HTTPStatus.NOT_ACCEPTABLE.name,
            details
        )


class HTTP409Conflict(HTTPBaseError):
    def __init__(self, response: ErrorEntry, details=None):
        HTTPBaseError.__init__(
            self,
            response,
            HTTPStatus.CONFLICT.value,
            HTTPStatus.CONFLICT.name,
            details
        )


class HTTP500InternalServerError(HTTPBaseError):
    def __init__(self, details=None):
        HTTPBaseError.__init__(
            self,
            BasicErrorCodes.UNDEFINED_ERROR,
            HTTPStatus.INTERNAL_SERVER_ERROR.value,
            HTTPStatus.INTERNAL_SERVER_ERROR.name,
            details
        )


class HTTP501NotImplemented(HTTPBaseError):
    def __init__(self, details=None):
        HTTPBaseError.__init__(
            self,
            BasicErrorCodes.METHOD_NOT_IMPLEMENTED,
            HTTPStatus.NOT_IMPLEMENTED.value,
            HTTPStatus.NOT_IMPLEMENTED.name,
            details
        )
