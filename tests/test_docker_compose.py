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


def test_clickhouse_has_required_password_envs(compose):
    """CH must set both CH_PASSWORD and CLICKHOUSE_PASSWORD.
    v26 entrypoint disables network access for default user if CLICKHOUSE_PASSWORD
    is absent — from_env XML config alone is not sufficient."""
    ch = compose["services"].get("clickhouse", {})
    env = ch.get("environment", {})
    env_keys = list(env.keys()) if isinstance(env, dict) else [e.split("=")[0] for e in env]
    assert "CH_PASSWORD" in env_keys, "CH_PASSWORD missing — used by from_env in trading-lab-user.xml"
    assert "CLICKHOUSE_PASSWORD" in env_keys, (
        "CLICKHOUSE_PASSWORD missing — v26 entrypoint disables network access without it"
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


def test_long_running_services_have_healthcheck(compose):
    """OPS-012: intraday_monitor and option_chain_intraday must have healthchecks.
    Without them, a hung container looks healthy to Docker — no alert fires."""
    for svc_name in ("intraday_monitor", "option_chain_intraday"):
        svc = compose["services"].get(svc_name, {})
        hc = svc.get("healthcheck", {})
        assert hc, f"Service '{svc_name}' has no healthcheck — OPS-012"
        test_cmd = str(hc.get("test", ""))
        assert "heartbeat" in test_cmd, (
            f"Service '{svc_name}' healthcheck must check heartbeat file, got: {test_cmd}"
        )


def test_dood_services_have_host_project_dir(compose):
    """scheduler and meta_pipeline use DooD (Docker-outside-of-Docker via socket).
    Both must export HOST_PROJECT_DIR so their docker compose run sub-invocations
    pass the correct HOST bind-mount path to the host Docker daemon.
    Without this, sub-containers get an empty /trading-lab and fail to read the
    compose file (root cause of meta_pipeline weekly 'exit 1' failures)."""
    for svc_name in ("scheduler", "meta_pipeline"):
        svc = compose["services"][svc_name]
        env = svc.get("environment", [])
        env_str = " ".join(str(e) for e in env)
        assert "HOST_PROJECT_DIR" in env_str, (
            f"Service '{svc_name}' must have HOST_PROJECT_DIR in environment "
            f"(use ${{PWD}} so the HOST path is injected at docker compose up time)"
        )


def test_migration_055_ttl_file_exists():
    """OPS-010: migration 055 adding TTL to market.options_chain must exist."""
    import os, glob
    migrations_dir = os.path.join(os.path.dirname(__file__), "..", "clickhouse", "migrations")
    files = glob.glob(os.path.join(migrations_dir, "055_*.sql"))
    assert files, "Migration 055 (options_chain TTL) not found — OPS-010"
    with open(files[0]) as f:
        content = f.read()
    assert "TTL" in content.upper(), "Migration 055 must contain TTL clause"
