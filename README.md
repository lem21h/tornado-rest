# TornadoWeb Rest

## Dependencies
- Tornado 5.1.1
- PyMongo 3.7.2
- Motor 2.0.0
- uvloop 0.11.3

## Usage

Create application that inherits from `RestAPIApp`
```python

class TestApp(RestAPIApp):
    pass

```

Create start method:
```python
if __name__ == "__main__":
    BASE_PATH = path.dirname(path.dirname(path.abspath(__file__)))
    
    get_app(DevelopmentConfig(BASE_PATH)).start()

```

where `get_app` looks like this:
```python
def get_app(config):

    return TestApp.build(config, ROUTING, DEFAULT_ROUTES). \
        registerService(FrontendService, config.frontend). \
        registerAsyncService(MongoAsyncConnection, config.mongo)
```

Routing comes from Tornado.

Default routes is a list of top level routes that are only maintenance related routes.

## Request handlers

Request handlers should inherit from `RestHandler` class.

`RestHandler` gives:
- `get_request_id` to get request IP
- simple CORS handling
- simple OPTIONS handling
- custom error handler as JSON response
- `return_ok` to return JSON "ok message"
- `self.request.json_body` to get JSON decoded request
- sort and pagination property helpers:
    - param_page
    - param_limit
    - param_order
    - param_sort
