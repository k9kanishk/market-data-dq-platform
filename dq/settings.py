from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DQ_", extra="ignore")
    database_url: str = "sqlite:///./dq.db"
    outputs_dir: str = "outputs"

settings = Settings()
