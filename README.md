# transactions-databases

A demonstration of transaction issues to look out for with encode/databases.

## Pre-requisites

Install dependencies and apply database migration:

```shell
poetry install
poetry shell
alembic upgrade head
```

### Run

You will have to run each test function in turn and some will need to be manually killed (i.e. with Ctrl+c) because they deadlock.

```shell
pytest -k test_function_name tests/
```
