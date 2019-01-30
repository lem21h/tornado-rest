# coding=utf-8
import logging
import time
from collections import namedtuple
from datetime import datetime
from inspect import isclass
from typing import Type, List, Dict

from tornado import httputil
from tornado.ioloop import IOLoop
from tornado.routing import ReversibleRuleRouter
from tornado.web import Application, RequestHandler

from config import NeuroAppConfig
from maio.core.configs import AppConfig
from maio.core.di import DI, ApiService
from maio.core.handlers import AclMixin, RestHandler
from maio.core.log import LOG_TORNADO_GENERAL

ApiServicesCont = namedtuple('ApiServicesCont', ('clazz', 'is_async', 'config'))
MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3


# monkey patching

def split_host_and_port_ex(netloc: str):
    try:
        host, port = netloc.split(':', 1)
        port = int(port)
    except ValueError:
        host = netloc
        port = None
    return host, port


httputil.split_host_and_port = split_host_and_port_ex


class _RestApplicationRouter(ReversibleRuleRouter):
    """Routing implementation used internally by `Application`.

    Provides a binding between `Application` and `RequestHandler`.
    This implementation extends `~.routing.ReversibleRuleRouter` in a couple of ways:
        * it allows to use `RequestHandler` subclasses as `~.routing.Rule` target and
        * it allows to use a list/tuple of rules as `~.routing.Rule` target.
        ``process_rule`` implementation will substitute this list with an appropriate
        `_RestApplicationRouter` instance.
    """

    def __init__(self, application, rules=None):
        assert isinstance(application, Application)
        self.application = application
        super(_RestApplicationRouter, self).__init__(rules)

    def process_rule(self, rule):
        rule = super(_RestApplicationRouter, self).process_rule(rule)

        if isinstance(rule.target, (list, tuple)):
            rule.target = _RestApplicationRouter(self.application, rule.target)

        return rule

    def get_target_delegate(self, target, request, **target_params):
        if isclass(target) and issubclass(target, RequestHandler):
            return self.application.get_handler_delegate(request, target, **target_params)

        return super(_RestApplicationRouter, self).get_target_delegate(target, request, **target_params)


def sig_handler(server, sig, frame):
    io_loop = IOLoop.instance()

    def stop_loop(deadline):
        now = time.time()
        if now < deadline and (io_loop.readers and io_loop.writers):
            logging.getLogger(LOG_TORNADO_GENERAL).info('Waiting for next tick')
            io_loop.add_timeout(now + 1, stop_loop, deadline)
        else:
            io_loop.stop()
            logging.getLogger(LOG_TORNADO_GENERAL).info('Shutdown finally')

    def shutdown():
        logging.info('Stopping http server')
        server.stop()
        logging.info('Will shutdown in %s seconds ...', MAX_WAIT_SECONDS_BEFORE_SHUTDOWN)
        stop_loop(time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN)

    logging.warning('Caught signal: %s', sig)
    io_loop.add_callback_from_signal(shutdown)


def _get_real_permissions_from_handler(clazz: Type[AclMixin], root_permissions):
    permissions = []

    if clazz.get != RestHandler.get:
        permissions.append(f"{root_permissions}_{AclMixin.METHOD_MAP['get']}")
    if clazz.post != RestHandler.post:
        permissions.append(f"{root_permissions}_{AclMixin.METHOD_MAP['post']}")
    if clazz.put != RestHandler.put:
        permissions.append(f"{root_permissions}_{AclMixin.METHOD_MAP['put']}")
    if clazz.delete != RestHandler.post:
        permissions.append(f"{root_permissions}_{AclMixin.METHOD_MAP['delete']}")
    extra = clazz.get_extra_permissions()
    if extra:
        permissions += extra
    return permissions


class RestAPIApp(Application):
    _config = None

    __slots__ = ['_startDate', '_services', '_reverse_routing_map', '_acl_list']

    def __init__(self, config: AppConfig, routing, base_routing):

        super().__init__(base_routing, config.web.host, None, **config.tornado.to_dict())
        RestAPIApp._config = config

        from datetime import datetime

        self._startDate = datetime.utcnow()
        self._services = []
        self._reverse_routing_map = {}
        self._acl_list = []
        self._build_routing(routing)

    def _build_routing(self, routing):
        from tornado.routing import Rule, AnyMatches, PathMatches

        routes = [Rule(PathMatches(route), _RestApplicationRouter(self, subRoutes)) for route, subRoutes in routing.items()]
        routes.append(Rule(AnyMatches(), self.wildcard_router))
        self.default_router = _RestApplicationRouter(self, routes)
        self._build_reverse_routing()

    def _build_reverse_routing(self):
        temp_map = {}
        temp_acl = []

        walker = [self.default_router]

        for elem in walker:
            if not hasattr(elem, 'rules'):
                continue
            for rule in elem.rules:
                if isinstance(rule.target, ReversibleRuleRouter):
                    walker.append(rule.target)
                else:
                    if rule.name:
                        temp_map[rule.name] = rule.handler_class
                    if rule.handler_class and issubclass(rule.handler_class, AclMixin) and rule.target_kwargs and 'permission' in rule.target_kwargs:
                        perm = _get_real_permissions_from_handler(rule.handler_class, rule.target_kwargs['permission'])
                        if perm:
                            temp_acl += perm

        self._reverse_routing_map = temp_map
        self._acl_list = tuple(set(temp_acl))

    @property
    def start_date(self) -> datetime:
        return self._startDate

    @property
    def config(self) -> NeuroAppConfig:
        return self._config

    @property
    def acl_list(self) -> List[str]:
        return self._acl_list

    @classmethod
    def build(cls, config, routing, base_routing):
        cls._config = config

        cls._initDI()
        cls._initLogging()

        return cls._initApp(routing, base_routing)

    @classmethod
    def _initLogging(cls):
        from maio.core.log import defineLogging

        cls._config.logging.relpath = cls._config.get_relative_path_safe(cls._config.logging.relpath)
        defineLogging(cls._config.logging)

    @classmethod
    def _initDI(cls):
        DI.initialize()
        DI.add('app_config', cls._config)

    @classmethod
    def _initApp(cls, routing, base_routing):
        app = cls(cls._config, routing, base_routing)
        app.ui_modules = {}
        return app

    def registerAsyncService(self, clazz, config=None):
        self._registerService(clazz, config, True)

        return self

    def registerService(self, clazz, config=None):
        self._registerService(clazz, config, False)

        return self

    def _registerService(self, clazz, config=None, is_async=False):
        if not issubclass(clazz, ApiService):
            raise TypeError('Provided class "%s" is not ApiService', clazz.__name__)
        self._services.append(ApiServicesCont(clazz, is_async, config))

    def _initServices(self, io_loop):

        for service in self._services:
            if service.is_async:
                inst = service.clazz(service.config, io_loop=io_loop)
            else:
                inst = service.clazz(service.config)

            DI.add(inst.getDIKey() if inst.getDIKey() else service.clazz, inst)

    def start(self):
        import asyncio
        import uvloop
        import signal
        from functools import partial
        from tornado.httpserver import HTTPServer
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        host = self._config.web.host
        port = self._config.web.port

        if not self.settings.get('debug'):
            server = HTTPServer(self, no_keep_alive=True)
            server.bind(port, host, 0, 2048, reuse_port=True)
            server.start(num_processes=None)
        else:
            server = self.listen(port)

        signal.signal(signal.SIGTERM, partial(sig_handler, server))
        signal.signal(signal.SIGINT, partial(sig_handler, server))

        msg = 'Starting %s webserver on %s:%d' % (self._config.web.name, host, port)
        logging.getLogger(LOG_TORNADO_GENERAL).info(msg)
        print(msg)
        loop = IOLoop.current()
        self._initServices(loop)
        loop.start()


def starter(env_map: Dict[str, Type[NeuroAppConfig]], default_path: str) -> NeuroAppConfig:
    from os import environ, path
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Application webserver")
    parser.add_argument('-c', default='dev', dest='config', help='Server configuration file')
    parser.add_argument('-t', default=None, dest='template_path', help='Templates path')
    parser.add_argument('-b', default=None, dest='base_path', help='Base path')

    cmd_args = parser.parse_args()

    env = (environ.get('ENVIRONMENT') if environ.get('ENVIRONMENT') else cmd_args.config).lower()
    base_path = cmd_args.base_path if cmd_args.base_path else default_path
    template_path = cmd_args.template_path if cmd_args.template_path else path.join(base_path, 'src', 'templates')

    config = env_map.get(env.lower(), env_map.get('default'))(base_path, template_path)
    status = config.enrich_with_json_file()
    if status > 0:
        print(f'Config enriched with status {status}')

    print(f'Base path = {config.BASE_PATH}')
    print(f'Templates path = {config.TEMPLATE_PATH}')

    return config
