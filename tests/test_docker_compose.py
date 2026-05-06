"""
Validates docker-compose.yml structure.
Catches: hardcoded host paths, exposed default credentials, bad config mounts.
"""
import os
import yaml
import pytest

COMPOSE_PATH = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")


@pytest.fixture(scope="module")
def compose():
    with open(COMPOSE_PATH) as f:
        return yaml.safe_load(f)


def test_compose_loads(compose):
    assert "services" in compose


def test_no_hardcoded_host_paths(compose):
    """COMPOSE_FILE must use /trading-lab (in-container path), not the host /home/... path."""
    bad = "/home/lavakumar/trading-lab"
    for name, svc in compose["services"].items():
        env = svc.get("environment", [])
        for entry in env:
            assert bad not in str(entry), (
                f"Service '{name}' has hardcoded host path in env: {entry}"
            )


def test_no_minio_default_password(compose):
    """MinIO password must not have a hardcoded default (:-password123)."""
    for name, svc in compose["services"].items():
        env = svc.get("environment", [])
        for entry in env:
            assert "password123" not in str(entry), (
                f"Service '{name}' exposes default MinIO password: {entry}"
            )


def test_clickhouse_config_mount_name(compose):
    """ClickHouse config must NOT be mounted as default-user.xml (conflicts with v26+ image)."""
    ch = compose["services"].get("clickhouse", {})
    for vol in ch.get("volumes", []):
        if "users.d" in str(vol):
            assert "default-user.xml" not in str(vol).split(":")[-1], (
                "ClickHouse config mounted as default-user.xml conflicts with image v26+; "
                "use trading-lab-user.xml instead"
            )


def test_compose_file_env_is_container_path(compose):
    """All COMPOSE_FILE env vars must use the in-container bind-mount path /trading-lab."""
    for name, svc in compose["services"].items():
        env = svc.get("environment", [])
        for entry in env:
            if isinstance(entry, str) and entry.startswith("COMPOSE_FILE="):
                assert entry.startswith("COMPOSE_FILE=/trading-lab"), (
                    f"Service '{name}' COMPOSE_FILE must be /trading-lab/..., got: {entry}"
                )


def test_all_pipeline_services_have_clickhouse_dependency(compose):
    """Pipeline run-once services must wait for clickhouse to be healthy."""
    # Long-running daemons (scheduler, dashboard) manage their own retry/reconnect;
    # they intentionally do NOT block on clickhouse at startup.
    skip = {"clickhouse", "minio", "jupyter", "portainer", "vscode",
            "dashboard", "scheduler", "recompute"}
    for name, svc in compose["services"].items():
        if name in skip:
            continue
        deps = svc.get("depends_on", {})
        if isinstance(deps, dict):
            assert "clickhouse" in deps, f"Service '{name}' missing depends_on clickhouse"
