"""Centralised settings, loaded from environment variables / .env file."""
from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = Field(default="postgres")
    postgres_port: int = Field(default=5432)
    postgres_user: str = Field(default="luigi")
    postgres_password: str = Field(default="luigi")
    postgres_db: str = Field(default="charts")

    elasticsearch_host: str = Field(default="elasticsearch")
    elasticsearch_port: int = Field(default=9200)
    es_index: str = Field(default="spotify_top_tracks")

    data_dir: Path = Field(default=Path("/app/data"))

    # iTunes Search API. No auth required, polite use only.
    # https://developer.apple.com/library/archive/documentation/AudioVideo/Conceptual/iTuneSearchAPI/
    itunes_search_url: str = Field(default="https://itunes.apple.com/search")
    itunes_per_artist_limit: int = Field(default=50)
    itunes_request_sleep_s: float = Field(default=0.5)

    @property
    def artists_file(self) -> Path:
        return self.data_dir / "seed" / "artists.txt"

    @property
    def output_dir(self) -> Path:
        return self.data_dir / "output"

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def es_url(self) -> str:
        return f"http://{self.elasticsearch_host}:{self.elasticsearch_port}"


settings = Settings()
