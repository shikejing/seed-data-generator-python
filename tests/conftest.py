"""Pytest fixtures: each test gets an app with its own temporary data dir."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from seedgen.api import create_app
from seedgen.config import Config


@pytest.fixture
def config(tmp_path):
    return Config(data_dir=tmp_path / "data")


@pytest.fixture
def app(config):
    return create_app(config)


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
def client2(config):
    """A second client backed by an independent app instance (same config)."""
    return TestClient(create_app(config))


def auth(name: str) -> dict:
    return {"X-User": name}


ALICE = "alice"
BOB = "bob"
ADMIN = "admin"
