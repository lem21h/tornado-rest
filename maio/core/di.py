from typing import Any, Dict


class ApiService(object):

    def __init__(self, config: Dict = None) -> None:
        super().__init__()

    @classmethod
    def getDIKey(cls):
        return None


class NoBinding:
    pass


class DI:
    _container: Dict[str, Any] = {}
    _initialized: bool = False

    def __init__(self):
        pass

    @classmethod
    def add(cls, name: str, container: Any):
        name = str(name)
        if name in cls._container:
            raise NameError('Container with given name "%s" already registered' % name)
        cls._container[name] = container
        return cls

    @classmethod
    def replace(cls, name: str, container: Any):
        name = str(name)

        cls._container[name] = container
        return cls

    @classmethod
    def initialize(cls):
        cls._initialized = True

    @classmethod
    def isInitialized(cls) -> bool:
        return cls._initialized

    @classmethod
    def get(cls, name: str) -> Any:
        name = str(name)
        container = cls._container.get(name, NoBinding)

        if container == NoBinding:
            raise RuntimeError(f"No binding defined for name [{name}]")
        else:
            if isinstance(container, type) or callable(container):
                return container()
            else:
                return container
