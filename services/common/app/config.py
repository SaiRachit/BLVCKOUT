import os


def get_env(name: str, default: str) -> str:
    return os.getenv(name, default)


def get_stream_url() -> str:
    """Redis Streams URL. Railway and other hosts often expose `REDIS_URL` only."""
    return os.getenv("STREAM_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
