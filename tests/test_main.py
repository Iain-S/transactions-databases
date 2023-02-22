from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
from asyncpg import UniqueViolationError
from fastapi.testclient import TestClient

from transac.main import app


@pytest.fixture()
def client():
    # context manager will invoke startup event
    with TestClient(app) as client:
        with patch("transac.main.SLEEP_FOR", 1):
            yield client


def test_read_root(client) -> None:
    futures = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        for _ in range(2):
            futures.append(executor.submit(client.get, "/"))
        with pytest.raises(UniqueViolationError):
            for future in futures:
                future.result()


def test_deadlock(client) -> None:
    client.get("/deadlock")


def test_deadlock2(client) -> None:
    client.get("/deadlock2")


def test_nested(client) -> None:
    futures = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for _ in range(4):
            futures.append(executor.submit(client.get, "/nested"))
        for future in futures:
            future.result()


def test_connections(client) -> None:
    client.get("/connections")
