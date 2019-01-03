# coding=utf-8
import functools
import json
import os
from http import HTTPStatus
from math import ceil

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.platform.asyncio import AsyncIOLoop
from tornado.testing import AsyncHTTPTestCase

from maio.core.data import CustomJsonEncoder
from maio.core.mongo import MongoSyncConnection, MongoUtils


def time_await(io_loop, fn, max_await_ts=2, debug=False):
    assert isinstance(io_loop, AsyncIOLoop)
    assert callable(fn)

    end_cond = int(ceil(max_await_ts / 0.25))
    ai = io_loop.asyncio_loop

    for step in range(0, end_cond):
        ret = fn()
        if debug:
            print('Await condition = %s' % ret)
        if ret:
            return True
        ai.run_until_complete(gen.sleep(0.1))
    return False


def _build_cookie(cookie, xsrf):
    headers = {'Content-Type': 'application/json'}
    if cookie:
        headers['Cookie'] = cookie
    if xsrf:
        headers['X-Xsrftoken'] = xsrf
        if cookie:
            headers['Cookie'] = '%s; _xsrf=%s' % (cookie, xsrf)
        else:
            headers['Cookie'] = '_xsrf=%s' % xsrf

    return headers


def extract_cookie(headers, cookie_name):
    if 'Set-Cookie' in headers:
        cookies = headers.get('Set-Cookie')
        pos1 = cookies.index(cookie_name)
        pos2 = cookies.index(';', pos1)
        return cookies[pos1:pos2]


def extract_xsrf(body):
    return body.get('xsrfToken')


class BaseRestTestCase(AsyncHTTPTestCase):
    POST = 'POST'
    GET = 'GET'
    PUT = 'PUT'
    DELETE = 'DELETE'
    OPTIONS = 'OPTIONS'
    HEAD = 'HEAD'

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self._config = self.get_config()
        # increase timeout from methods
        os.environ['ASYNC_TEST_TIMEOUT'] = '30'
        self._userXsrf = None
        self._userCookie = None

    def get_config(self):
        raise NotImplementedError()

    def get_app(self):
        raise NotImplementedError()

    def setUp(self):
        super().setUp()
        self._app._initServices(IOLoop.current())
        # clean up database
        MongoUtils(MongoSyncConnection(self._config.mongo)).drop_collections()

    def run_as_sync(self, fn, io_loop=None, *args, **kwargs):
        io_loop = self.io_loop if io_loop is None else io_loop
        return io_loop.run_sync(functools.partial(fn, *args, **kwargs))

    @property
    def passwordSalt(self):
        return self._config.security.get('password_salt')

    def _makeRawCall(self, url, method='GET', json_body=None, cookie=None, xsrf=None, expected_code=200):
        body = json.dumps(json_body, cls=CustomJsonEncoder) if json_body else None
        response = self.fetch(url, method=method, headers=_build_cookie(cookie, xsrf), allow_nonstandard_methods=True, body=body)

        self.assertEqual(expected_code, response.code, 'Returned unexpected code %d. Response: %s' % (response.code, response.body))
        if method != self.OPTIONS and response.headers.get('Content-Type', '').startswith('application/json'):
            response.json_body = json.loads(response.body.decode())
        else:
            response.json_body = None

        return response

    def makeApiCall(self, url, method='GET', json_body=None, cookie=None, xsrf=None, expected_status=HTTPStatus.OK):
        if not isinstance(expected_status, HTTPStatus):
            raise TypeError('Expected status definition')
        response = self._makeRawCall(url, method, json_body, cookie, xsrf, expected_status.value)

        return response.json_body if response.json_body else response.body

    def makeSessionApiCall(self, url, method='GET', json_body=None, expected_status=HTTPStatus.OK):
        if not isinstance(expected_status, HTTPStatus):
            raise TypeError('Expected status definition')
        if not self._userCookie and not self._userXsrf:
            self.fail('Start session first')

        xsrf = self._userXsrf if method not in (self.GET, self.HEAD, self.OPTIONS) else None
        response = self._makeRawCall(url, method, json_body, self._userCookie, xsrf, expected_status.value)

        return response.json_body if response.json_body else response.body

    # call methods

    def apiPost(self, controller_name, json_body, cookie=None, xsrf=None, expected_status=HTTPStatus.OK, query=None):
        return self.makeApiCall(self.build_url(controller_name, query), method=self.POST, json_body=json_body, cookie=cookie, xsrf=xsrf,
                                expected_status=expected_status)

    def apiPut(self, controller_name, json_body, cookie=None, xsrf=None, expected_status=HTTPStatus.OK, query=None):
        return self.makeApiCall(self.build_url(controller_name, query), method=self.PUT, json_body=json_body, cookie=cookie, xsrf=xsrf,
                                expected_status=expected_status)

    def apiGet(self, controller_name, cookie=None, xsrf=None, expected_status=HTTPStatus.OK, query=None):
        return self.makeApiCall(self.build_url(controller_name, query), self.GET, cookie=cookie, xsrf=xsrf, expected_status=expected_status)

    def apiDelete(self, controller_name, cookie=None, xsrf=None, expected_status=HTTPStatus.OK, query=None):
        return self.makeApiCall(self.build_url(controller_name, query), self.DELETE, cookie=cookie, xsrf=xsrf, expected_status=expected_status)

    def secApiPost(self, controller_name, json_body, expected_status=HTTPStatus.OK, query=None):
        return self.makeSessionApiCall(self.build_url(controller_name, query), method=self.POST, json_body=json_body, expected_status=expected_status)

    def secApiPut(self, controller_name, json_body, expected_status=HTTPStatus.OK, query=None):
        return self.makeSessionApiCall(self.build_url(controller_name, query), method=self.PUT, json_body=json_body, expected_status=expected_status)

    def secApiGet(self, controller_name, expected_status=HTTPStatus.OK, query=None):
        return self.makeSessionApiCall(self.build_url(controller_name, query), self.GET, expected_status=expected_status)

    def secApiDelete(self, controller_name, expected_status=HTTPStatus.OK, query=None):
        return self.makeSessionApiCall(self.build_url(controller_name, query), self.DELETE, expected_status=expected_status)

    def build_url(self, params, query=None):
        if params:
            if isinstance(params, tuple):
                url = self._app.reverse_url(*params)
            else:
                url = self._app.reverse_url(params)
            return f"{url}?{query}" if query else url
        else:
            raise ValueError('Expected name of the controller')

    # helpers testing methods

    def assertResponse(self, response, status_message):
        self.assertIsNotNone(response)
        self.assertIn('status', response)
        self.assertEqual(status_message, response['status'])

    def assertOkResponse(self, response, response_contains_keys=None):
        self.assertResponse(response, 'OK')
        if response_contains_keys and isinstance(response_contains_keys, (list, tuple, set)):
            for key in response_contains_keys:
                self.assertIn(key, response)

    def assertErrorResponse(self, response, error_code=None):
        self.assertResponse(response, 'ERROR')
        self.assertIn('error_code', response)
        if error_code:
            if isinstance(error_code, tuple):
                self.assertEqual(error_code[0], response['error_code'])
                self.assertEqual(error_code[1], response['message'])
            else:
                self.assertEqual(error_code, response['error_code'])
