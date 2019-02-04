# coding=utf-8
import json
import logging
import os
from typing import Any, Dict, Callable, Optional

from maio.core.data import VO
from maio.core.handlers import NotFoundRestHandler


class _BaseConfig(VO):
    __slots__ = ()

    def get(self, key: str) -> Any:
        if key in self.get_fields():
            return getattr(self, key)
        else:
            raise AttributeError(f'No key [{key}] in object')

    def to_dict(self) -> Dict[str, Any]:
        out = {}
        for key in self.get_fields():
            val = getattr(self, key)
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
        'friendfeed_consumer_key', 'friendfeed_consumer_secret',
        'google_consumer_key', 'google_consumer_secret', 'google_oauth',
        'facebook_api_key', 'facebook_api_secret'
    )

    def __init__(self) -> None:
        super().__init__()
        self.twitter_consumer_key: str = None
        self.twitter_consumer_secret: str = None
        self.friendfeed_consumer_key: str = None
        self.friendfeed_consumer_secret: str = None
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


class AppConfig:
    BASE_PATH = None
    TEMPLATE_PATH = None

    __slots__ = ('tornado', 'web', 'logging', 'cors', 'security', 'locale', 'apiVersion')

    def __init__(self, base_path: str, template_path: Optional[str] = None, api_version: Optional[str] = None) -> None:
        super().__init__()

        self.set_base_path(base_path)
        if template_path:
            self.set_template_path(template_path)

        self.apiVersion = api_version
        self._load_version()
        self.tornado = TornadoSettings()
        self.web = WebSettings()
        self.logging = LoggerConfig()
        self.cors = CorsConfig()
        self.security = SecurityConfig()
        self.locale = LocaleConfig()

    @property
    def env(self) -> str:
        return self.__class__.__name__

    @classmethod
    def set_template_path(cls, new_path):
        cls.TEMPLATE_PATH = os.path.abspath(new_path)
        if not os.path.exists(cls.TEMPLATE_PATH):
            raise ValueError(f'Cannot find TEMPLATE PATH {new_path}')

    @classmethod
    def set_base_path(cls, new_path):
        cls.BASE_PATH = os.path.abspath(new_path)
        cls.TEMPLATE_PATH = os.path.join(cls.BASE_PATH, 'src', 'templates')
        if not os.path.exists(cls.BASE_PATH):
            raise ValueError(f'Cannot find BASE PATH {new_path}')

    def on_update(self, data: Dict[str, Any]) -> None:
        if data.get('cors') and data['cors'].get('allowed_origin') and isinstance(data['cors']['allowed_origin'], list):
            self.cors.allowed_origin = data['cors']['allowed_origin']
        if data.get('web') and data['web'].get('port'):
            self.web.port = data['web']['port']

    def enrich_with_json_file(self, config_path: Optional[str] = None) -> int:
        if not config_path:
            config_path = os.path.join(self.BASE_PATH, 'config.json')
        if os.path.exists(config_path):
            try:
                with open(config_path, 'rb') as fp:
                    data = json.load(fp)
                if data and isinstance(data, dict):
                    self.on_update(data)
                    return 1
                else:
                    return -1
            except Exception as ex:
                print(f'While updating config exception has occurred {ex}')
                return 2
        else:
            return -1

    def to_dict(self):
        return {k: self.__getattribute__(k).to_dict() for k in self.__slots__[1:]}

    @property
    def base_path(self) -> str:
        return self.BASE_PATH

    def get_relative_path(self, rel_path: str) -> str:
        return os.path.join(self.BASE_PATH, rel_path)

    def get_relative_path_safe(self, rel_path: str) -> str:
        return rel_path if rel_path.startswith('/') else os.path.join(self.BASE_PATH, rel_path)

    def _load_version(self):
        ver_file = os.path.join(self.BASE_PATH, 'version.api')
        if os.path.isfile(ver_file):
            with open(ver_file, 'rb') as inFile:
                self.apiVersion = inFile.read().strip()
