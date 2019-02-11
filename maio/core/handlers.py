# coding=utf-8
import email
import json
import logging
import time
import traceback
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Optional

import tornado
import tornado.httputil
import tornado.web
from gridfs import GridOut
from tornado import httputil
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler

from maio.core.data import CustomJsonEncoder
from maio.core.exceptions import HTTPBaseError, HTTP400BadRequestError, HTTP406NotAcceptable, BasicErrorCodes, HTTP403ForbiddenError, HTTP404NotFoundError
from maio.core.helpers import parse_uuid, parse_bool, parse_date_to_unix_ts
from maio.core.log import LOG_EXCEPTIONS, LOG_MAIN
from maio.core.validators import SimpleValidator


def _log_exception(exception, uri, headers_iter, body, trace_log):
    detail_dict = {
        'ERROR': exception,
        'URL': uri,
        'HEADERS': ' || '.join(['[{}]:"{}"'.format(k, v) for k, v in headers_iter])
    }
    if body:
        detail_dict['BODY'] = body
    if trace_log:
        detail_dict['TRACE'] = trace_log

    logging.getLogger(LOG_EXCEPTIONS).error(u'\n{}\n'.format(
        u'\n'.join(['{}: {}'.format(k, v) for k, v in detail_dict.items()])
    ))


def _build_error_response(message: str, details=None, error_code: int = HTTPStatus.INTERNAL_SERVER_ERROR.value):
    response = {
        'status': u'ERROR',
        'error_code': error_code,
        'message': message
    }

    if details:
        response['details'] = details

    return response


def _error_response_append_debug(response, req_args=None, req_body=None, header_iter=None, trace_log=None):
    response['debug'] = {}
    if req_args:
        response['debug']['arguments'] = req_args
    if req_body:
        response['debug']['body'] = req_body
    if header_iter:
        response['debug']['headers'] = {k: v for k, v in header_iter}
    if trace_log:
        response['debug']['trace'] = trace_log
    return response


class AclMixin:
    METHOD_MAP = {
        'post': 'save',
        'get': 'fetch',
        'put': 'update',
        'delete': 'remove'
    }

    __slots__ = ['_permission', '_user']

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._permission = kwargs.get('permission')
        self._user = None

    @classmethod
    def get_extra_permissions(cls):
        return []

    @classmethod
    def full_permission_name(cls, permission, method):
        method = method.lower()
        return '%s_%s' % (permission, cls.METHOD_MAP[method]) if method in cls.METHOD_MAP else permission

    def _check_perform_action(self, method, available_permissions):
        if not self._permission:
            return True
        real_perm = self.full_permission_name(self._permission, method)

        if not available_permissions or (real_perm and real_perm not in available_permissions):
            self._access_denied_exception(real_perm)

    @staticmethod
    def _access_denied_exception(real_perm):
        raise HTTP403ForbiddenError(BasicErrorCodes.CANNOT_PERFORM_THIS_ACTION, details="No permission %s" % real_perm)

    def get_user(self):
        return self._user


def get_available_permissions(app):
    return app.acl_list


class RestHandler(RequestHandler):
    CORS_HEADERS = []
    _LOG_INST = logging.getLogger(LOG_MAIN)

    def __init__(self, application, request, **kwargs):
        self.application = application
        self.request = request
        self._headers_written = False
        self._finished = False
        self._auto_finish = True
        self._transforms = None  # will be set in _execute
        self._prepared_future = None
        self._headers = None  # type: httputil.HTTPHeaders
        self.path_args = None
        self.path_kwargs = None
        self.clear()
        self.request.connection.set_close_callback(self.on_connection_close)
        self.initialize(**kwargs)

    @classmethod
    def log(cls, level=logging.INFO, message=None, *args, **kwargs):

        cls._LOG_INST.log(level, message, args, **kwargs)

    @classmethod
    def get_rev_name(cls):
        raise cls.__name__

    @property
    def param_page(self):
        return self.get_argument('page', None)

    @property
    def param_limit(self):
        return self.get_argument('limit', None)

    @property
    def param_order(self):
        return self.get_argument('order', None)

    @property
    def param_sort(self):
        return self.get_argument('sort', None)

    def get_bool_param(self, field_name: str, default: Optional[bool] = None) -> bool:
        return parse_bool(self.get_argument(field_name, None), default)

    def get_date_unix_param(self, field_name, default=None):
        return parse_date_to_unix_ts(self.get_argument(field_name, default))

    @property
    def config(self):
        return self.application.config

    def data_received(self, chunk):
        pass

    def initialize(self, **kwargs):
        super(RestHandler, self).initialize()

    def clear(self):
        self._headers = httputil.HTTPHeaders({
            'Server': self.config.web.get('name'),
            'Content-Type': 'application/json; charset=UTF-8',
            'Date': httputil.format_timestamp(time.time()),
        })
        self.set_default_headers()
        self._write_buffer = []
        self._status_code = HTTPStatus.OK.value
        self._reason = HTTPStatus.OK.value

    def get_request_ip(self):
        if self.request.remote_ip == '127.0.0.1' and 'X-Forwarded-For' in self.request.headers:
            return self.request.headers.get('X-Forwarded-For')
        else:
            return self.request.remote_ip

    def set_default_headers(self):
        if self.config.cors.get('allowed_origin'):
            self._build_cors_origin(self.config.cors.get('allowed_origin'))
        if self.config.cors.get('allowed_headers'):
            self.set_header('Access-Control-Allow-Headers', ', '.join(self.config.cors.get('allowed_headers') + self.CORS_HEADERS))

        self.set_header('Access-Control-Max-Age', 86400)
        self.set_header('Access-Control-Allow-Credentials', 'true')

    def _build_cors_origin(self, allow_origin=None):
        cors = None
        if allow_origin == '*':
            cors = '*'
        elif self.request.headers.get('Origin') and isinstance(allow_origin, (tuple, set, list)):
            o = self.request.headers.get('Origin')
            if o in allow_origin:
                cors = o
            else:
                cors = allow_origin[0]

        if cors:
            self.set_header('Access-Control-Allow-Origin', cors)

    def options(self, *args, **kwargs):
        self.set_header('Access-Control-Allow-Methods', 'GET, POST, DELETE, PUT')
        self.set_status(HTTPStatus.NO_CONTENT.value)
        self.finish()

    def write_error(self, status_code, **kwargs):
        exc_info = kwargs.get('exc_info')
        exception = exc_info[1] if exc_info else None
        trace_log = None

        if isinstance(exception, tornado.web.HTTPError):
            if isinstance(exception, HTTPBaseError):
                response = _build_error_response(exception.message, exception.details, exception.code)
            else:
                response = _build_error_response(exception.reason, exception.log_message, exception.status_code)
        else:
            response = _build_error_response(tornado.httputil.responses.get(status_code, 'Unknown error'), type(exception).__name__)
            trace_log = u''.join(traceback.format_exception(*exc_info)) if exc_info else None

        ex_cfg = self.config.logging.get('exceptions')
        if ex_cfg and ex_cfg.get('enabled') and (status_code == 500 or (ex_cfg.get('codes') and status_code in ex_cfg.get('codes'))):
            _log_exception(exception, self.request.uri, self.request.headers.get_all(), self.request.body, trace_log)

        if self.settings.get('debug'):
            if isinstance(self.request.body, bytes):
                body = self.request.body.decode()
            elif isinstance(self.request.body, str):
                body = self.request.body
            else:
                body = str(self.request.body)
            _error_response_append_debug(response, self.request.arguments, body, self.request.headers.get_all(), trace_log)

        self.finish(json.dumps(response, cls=CustomJsonEncoder, ensure_ascii=False))

    def return_ok(self, dict_response=None, total_count=None, status: HTTPStatus = HTTPStatus.OK):
        self.set_status(status.value)
        if dict_response:
            response = dict_response
            response['status'] = 'OK'
        else:
            response = {'status': u'OK'}
        if total_count is not None:
            response['totalCount'] = int(total_count)

        self.finish(json.dumps(response, cls=CustomJsonEncoder, ensure_ascii=False).replace("</", "<\\/"))

    async def prepare(self):
        super(RestHandler, self).prepare()
        if self.request.method in ('POST', 'PUT', 'PATCH'):
            if 'application/json' in self.request.headers.get('Content-Type', ''):
                self.request.json_body = ReqUtils.processJsonBody(self.request.body)
            else:
                raise HTTP406NotAcceptable(BasicErrorCodes.INVALID_CONTENT)

    def check_etag_header(self):
        # disable etag checking
        pass


class ReqUtils:
    @staticmethod
    def run_in_background(fn, *args, **kwargs):
        IOLoop.current().spawn_callback(fn, args, **kwargs)

    @staticmethod
    def processJsonBody(content):
        if hasattr(content, '__len__') and len(content) > 0:
            try:
                return json.loads(content.decode(encoding='UTF-8') if isinstance(content, bytes) else content)
            except ValueError:
                raise HTTP400BadRequestError(BasicErrorCodes.INVALID_CONTENT)
        else:
            return {}

    @staticmethod
    def validate(data, validators):
        val_res = SimpleValidator.validate(data, validators)
        if val_res.has_errors():
            raise HTTP400BadRequestError(BasicErrorCodes.VALIDATION_ERROR, details=val_res.errors)
        return val_res.result

    @staticmethod
    def tryParseUuid(uuid_str: str, raise_error=True):
        uuid_obj = parse_uuid(uuid_str)
        if raise_error and not uuid_obj:
            raise HTTP400BadRequestError(BasicErrorCodes.BAD_UUID)
        return uuid_obj

    @staticmethod
    async def send_file(h_request: RequestHandler, fp: GridOut, cache_time: int = 0):
        # If-Modified-Since header is only good to the second.
        modified = fp.upload_date.replace(microsecond=0)
        h_request.set_header("Last-Modified", modified)

        # MD5 is calculated on the MongoDB server when GridFS file is created
        h_request.set_header("Etag", f'"{fp.md5}"')

        mime_type = fp.content_type
        if not mime_type:
            mime_type = fp.metadata.get('contentType')

        # Starting from here, largely a copy of StaticFileHandler
        if mime_type:
            h_request.set_header("Content-Type", mime_type)

        if cache_time > 0:
            h_request.set_header("Expires", datetime.utcnow() + timedelta(seconds=cache_time))
            h_request.set_header("Cache-Control", f"max-age={cache_time}")
        else:
            h_request.set_header("Cache-Control", "public")

        # Check the If-Modified-Since, and don't send the result if the
        # content has not been modified
        ims_value = h_request.request.headers.get("If-Modified-Since")
        if ims_value is not None:
            date_tuple = email.utils.parsedate(ims_value)

            # If our MotorClient is tz-aware, assume the naive ims_value is in
            # its time zone.
            if_since = datetime.fromtimestamp(time.mktime(date_tuple)).replace(tzinfo=modified.tzinfo)

            if if_since >= modified:
                h_request.set_status(304)
                return

        # Same for Etag
        etag = h_request.request.headers.get("If-None-Match")
        if etag is not None and etag.strip('"') == fp.md5:
            h_request.set_status(304)
            return

        h_request.set_header("Content-Length", fp.length)
        await fp.stream_to_handler(h_request)
        h_request.finish()


class NotFoundRestHandler(RestHandler):
    def check_xsrf_cookie(self):
        pass

    def get(self, path=None, **kwargs):
        raise HTTP404NotFoundError(BasicErrorCodes.GENERAL_NOT_FOUND)

    def post(self, path=None, **kwargs):
        raise HTTP404NotFoundError(BasicErrorCodes.GENERAL_NOT_FOUND)

    def put(self, path=None, **kwargs):
        raise HTTP404NotFoundError(BasicErrorCodes.GENERAL_NOT_FOUND)

    def delete(self, path=None, **kwargs):
        raise HTTP404NotFoundError(BasicErrorCodes.GENERAL_NOT_FOUND)
