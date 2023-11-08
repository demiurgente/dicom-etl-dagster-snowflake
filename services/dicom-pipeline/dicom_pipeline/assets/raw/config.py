from dagster import Config, Optional, List, PermissiveConfig
class RawConfig(Config):
    asset: str
    columns: list[str]
    files: list[str]
    partition_column: str = None
