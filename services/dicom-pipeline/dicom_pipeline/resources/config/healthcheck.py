import os
from pydantic import BaseModel, BaseSettings, SecretStr
from pydantic_vault import vault_config_settings_source
from .lib import DAGSTER_ENV, PROJECT_NAME

# monitor cron jobs, failed heartbeats using healthcheck service
healthcheck_etl_channel_map = {
    "local": f"{PROJECT_NAME}-etl-{DAGSTER_ENV}",
	"stage": f"{PROJECT_NAME}-etl-{DAGSTER_ENV}",
	"prod": f"{PROJECT_NAME}-etl-{DAGSTER_ENV}",
}
route = healthcheck_etl_channel_map[PROJECT_NAME]

HEALTHCHECK_SECRET_KEY = os.getenv("HC_SECRET_KEY", "")
HEALTHCHECK_VAULT_URL = os.environ.get("HEALTHCHECK_VAULT_URL", "")
HEALTHCHECK_SECRET_PATH = f"secret-data/pipelines/{PROJECT_NAME}/{route}/healthchecks-io-check-id"

class VaultDBCredentials(BaseModel):
    username: str
    password: SecretStr


class VaultBaseSettings(BaseSettings):
    class Config:
        vault_url: str = HEALTHCHECK_VAULT_URL

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                vault_config_settings_source,
                init_settings,
                env_settings,
                file_secret_settings,
            )
