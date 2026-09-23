from fastapi.testclient import TestClient

from app.api import create_app


def test_root_unchanged():
    client = TestClient(create_app())
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"Hello": "World"}
