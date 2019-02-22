# coding=utf-8
from typing import Optional, Dict, Type

from tornado.routing import Rule, PathMatches

from maio.core.handlers import RestHandler


class URLSpecV2(Rule):
    __slots__ = ('regex', 'handler_class', 'kwargs')
    """Specifies mappings between URLs and handlers.

    .. versionchanged: 4.5
       `URLSpec` is now a subclass of a `Rule` with `PathMatches` matcher and is preserved for
       backwards compatibility.
    """

    def __init__(self, pattern: str, handler: Type[RestHandler], parameters: Optional[Dict] = None):
        """Parameters:

        * ``pattern``: Regular expression to be matched. Any capturing
          groups in the regex will be passed in to the handler's
          get/post/etc methods as arguments (by keyword if named, by
          position if unnamed. Named and unnamed capturing groups
          may not be mixed in the same rule).

        * ``handler``: `~.web.RequestHandler` subclass to be invoked.

        * ``parameters`` (optional): A dictionary of additional arguments
          to be passed to the handler's constructor.

        """
        name = None
        if issubclass(handler, RestHandler):
            name = handler.get_rev_name()

        super().__init__(PathMatches(pattern), handler, parameters, name)

        self.regex = self.matcher.regex
        self.handler_class = self.target
        self.kwargs = parameters

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.regex.pattern}, {self.handler_class}, kwargs={self.kwargs:r}, name={self.name})'


url = URLSpecV2
