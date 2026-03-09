from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    worker_concurrency: int = 4
    heartbeat_interval: int = 10
    heartbeat_ttl: int = 30
    retry_max: int = 3
    reaper_interval: int = 30
    reaper_timeout: int = 60
    scheduler_interval: int = 1

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
