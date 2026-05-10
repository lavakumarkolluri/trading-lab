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


def test_clickhouse_healthcheck_tests_auth(compose):
    """CH healthcheck must test auth (clickhouse-client), not just /ping.
    /ping returns 200 even with wrong password — dependent containers would
    start and fail with auth error 194, as happened May 7-10 2026."""
    ch = compose["services"].get("clickhouse", {})
    hc = ch.get("healthcheck", {})
    test_cmd = str(hc.get("test", ""))
    assert "clickhouse-client" in test_cmd, (
        "CH healthcheck must use clickhouse-client to verify auth, not wget /ping"
    )
    assert "password" in test_cmd.lower(), (
        "CH healthcheck must pass --password to actually test authentication"
    )


def test_clickhouse_has_official_password_env(compose):
    """CH must set CLICKHOUSE_PASSWORD (official env) in addition to CH_PASSWORD.
    from_env XML resolution can fail silently on container recreate; the official
    env var sets the password directly via the entrypoint as belt-and-suspenders."""
    ch = compose["services"].get("clickhouse", {})
    env = ch.get("environment", {})
    env_keys = list(env.keys()) if isinstance(env, dict) else [e.split("=")[0] for e in env]
    assert "CLICKHOUSE_PASSWORD" in env_keys, (
        "ClickHouse must set CLICKHOUSE_PASSWORD env var for reliable auth on recreate"
    )


def test_clickhouse_image_is_pinned(compose):
    """CH image must be pinned to a specific version, not :latest.
    :latest can pull a breaking change silently."""
    ch = compose["services"].get("clickhouse", {})
    image = ch.get("image", "")
    assert ":latest" not in image, (
        f"ClickHouse image must be pinned (e.g. 26.4), not :latest. Got: {image}"
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
