from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_prefix="MINICELERY_",
    )
    debug: bool = True
    log_level: str = "INFO"
    log_file: Path | None = None #BASE_DIR / "logs" / "minicelery.log" # None - only console
    log_max_bytes: int = 10 * 1024 * 1024
    log_backup_count: int = 5
    #concurrency: int = Field(4, ge=1)
    queue: str = "minicelery:queue:default:1"



settings = Settings()