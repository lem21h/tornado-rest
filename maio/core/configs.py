# coding=utf-8
import logging
import os
from typing import Any, Dict, Callable

from maio.core.handlers import NotFoundRestHandler


class _BaseConfig(object):
    __slots__ = ()

    def get(self, key: str) -> Any:
        if key in self.__slots__:
            return self.__getattribute__(key)
        else:
            raise AttributeError('No key [%s] in object' % key)

    def to_dict(self) -> Dict[str, Any]:
        out = {}
        for key in self.__slots__:
            val = self.__getattribute__(key)
            if val is not None:
                out[key] = val
        return out


class TornadoGeneralSettings(_BaseConfig):
    __slots__ = (
        'autoreload', 'debug', 'default_handler_class', 'default_handler_args', 'compress_response', 'log_function', 'serve_traceback',
        'ui_modules', 'ui_methods', 'websocket_ping_interval', 'websocket_ping_timeout', 'websocket_max_message_size'
    )

    def __init__(self) -> None:
        super().__init__()
        self.autoreload: bool = False
        self.debug: bool = False
        self.default_handler_class = NotFoundRestHandler
        self.default_handler_args: dict = None
        self.compress_response: bool = False
        self.log_function: Callable = None
        self.serve_traceback: bool = True
        self.ui_modules = None
        self.ui_methods = None
        self.websocket_ping_interval: int = None
        self.websocket_ping_timeout: int = None
        self.websocket_max_message_size: int = None


class TornadoSecuritySettings(_BaseConfig):
    __slots__ = ('cookie_secret', 'key_version', 'login_url', 'xsrf_cookies', 'xsrf_cookie_version', 'xsrf_cookie_kwargs')

    def __init__(self) -> None:
        super().__init__()
        self.cookie_secret: str = None
        self.key_version: int = None
        self.login_url: str = None
        self.xsrf_cookies: bool = True
        self.xsrf_cookie_version: int = None
        self.xsrf_cookie_kwargs: dict = None


class TornadoAuthSettings(_BaseConfig):
    __slots__ = (
        'twitter_consumer_key', 'twitter_consumer_secret',
        'friendfeed_consumer_key', 'friendfeed_consuler_secret',
        'google_consumer_key', 'google_consumer_secret', 'google_oauth',
        'facebook_api_key', 'facebook_api_secret'
    )

    def __init__(self) -> None:
        super().__init__()
        self.twitter_consumer_key: str = None
        self.twitter_consumer_secret: str = None
        self.friendfeed_consumer_key: str = None
        self.friendfeed_consuler_secret: str = None
        self.google_consumer_key: str = None
        self.google_consumer_secret: str = None
        self.facebook_api_key: str = None
        self.facebook_api_secret: str = None
        self.google_oauth: dict = None


class TornadoTemplateSettings(_BaseConfig):
    __slots__ = ('autoescape', 'compiled_template_cache', 'template_path', 'template_loader', 'template_whitespace')

    def __init__(self) -> None:
        super().__init__()
        self.autoescape: str = None
        self.compiled_template_cache: bool = True
        self.template_path: str = None
        self.template_loader = None
        self.template_whitespace: str = None


class TornadoStaticFileSettings(_BaseConfig):
    __slots__ = ('static_hash_cache', 'static_path', 'static_url_prefix', 'static_handler_class', 'static_handler_args')

    def __init__(self) -> None:
        super().__init__()
        self.static_hash_cache: bool = True
        self.static_path: str = None
        self.static_url_prefix: str = '/static/'
        self.static_handler_class = None
        self.static_handler_args = None


class TornadoSettings(object):
    __slots__ = ('general', 'security', 'external_auth', 'template', 'static_files')

    def __init__(self) -> None:
        super().__init__()

        # General settings
        self.general = TornadoGeneralSettings()

        # Authentication and security settings
        self.security = TornadoSecuritySettings()
        self.external_auth = TornadoAuthSettings()

        # template settings
        self.template = TornadoTemplateSettings()

        # static file settings
        self.static_files = TornadoStaticFileSettings()

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        return {
            **self.general.to_dict(),
            **self.security.to_dict(),
            **self.external_auth.to_dict(),
            **self.template.to_dict(),
            **self.static_files.to_dict()
        }


class WebSettings(_BaseConfig):
    __slots__ = ('name', 'port', 'host')

    def __init__(self) -> None:
        super().__init__()
        self.name = 'Rest API Server'
        self.port = 8080
        self.host = '0.0.0.0'


class LoggerConfig(_BaseConfig):
    __slots__ = ('level', 'output_console', 'output_file', 'relpath', 'logformat', 'exceptions')

    def __init__(self) -> None:
        super().__init__()

        self.level = logging.DEBUG
        self.output_console = True
        self.output_file = False
        self.relpath = 'logs'
        self.logformat = "%(asctime)-12s: - %(levelname)-8s]] - %(message)s"
        self.exceptions = {
            'enabled': True,
            'codes': (500, 501, 502, 503)
        }


class CorsConfig(_BaseConfig):
    __slots__ = ('allowed_headers', 'allowed_origin')

    def __init__(self) -> None:
        super().__init__()
        self.allowed_headers = ["X-Lang", "Content-Type", "Authorization", "X-Filename", "x-requested-with"]
        self.allowed_origin = '*'


class SecurityConfig(_BaseConfig):
    __slots__ = ('session_timeout', 'single_session', 'password_salt', 'use_https')

    def __init__(self) -> None:
        super().__init__()

        self.session_timeout = 900
        self.single_session = False,
        self.password_salt = 'XxXxXxXxXxXxX'
        self.use_https = False


class LocaleConfig(_BaseConfig):
    __slots__ = ('domain', 'path', 'default_locale', 'default_country', 'default_time_format', 'default_timezone')

    def __init__(self) -> None:
        super().__init__()
        self.domain = ''
        self.path = 'locale'
        self.default_locale = 'en_EN'
        self.default_country = 'POL'
        self.default_time_format = "%Y-%m-%dT%H:%M:%S"
        self.default_timezone = 'GMT'


class TornadoConfig(object):
    __slots__ = ('_base_path', 'tornado', 'web', 'logging', 'cors', 'security', 'locale')

    @property
    def env(self) -> str:
        return self.__class__.__name__

    def __init__(self, base_path) -> None:
        super().__init__()
        self._base_path = base_path
        self.tornado = TornadoSettings()
        self.web = WebSettings()
        self.logging = LoggerConfig()
        self.cors = CorsConfig()
        self.security = SecurityConfig()
        self.locale = LocaleConfig()

    def to_dict(self):
        return {k: self.__getattribute__(k).to_dict() for k in self.__slots__[1:]}

    @property
    def basePath(self) -> str:
        return self._base_path

    def baseRelativePath(self, rel_path: str) -> str:
        return os.path.join(self._base_path, rel_path)

    def baseRelativePathSafe(self, rel_path: str) -> str:
        return rel_path if rel_path.startswith('/') else os.path.join(self._base_path, rel_path)
