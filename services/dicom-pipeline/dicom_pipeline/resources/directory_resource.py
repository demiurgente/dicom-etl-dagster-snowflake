from typing import Optional
import shutil
from pathlib import Path, PosixPath
from pydantic import Field
from dagster import (
	ConfigurableResource,
	InitResourceContext,
	usable_as_dagster_type
)


@usable_as_dagster_type
class DagsterPath(PosixPath):
    pass

def current_dir_str():
    return str(Path.cwd())


class BaseDirectoryResource(ConfigurableResource):
    outputs_root_dir: str = Field(
        default_factory=current_dir_str,
        description=(
            "Base directory used for creating a results folder. Should be configured to"
            " allow writing by the Dagster user"
        ),
    )
    dir_prefix: Optional[str] = Field(
        default=None,
        description=(
            "An optional directory name to nest the resource directory within. "
            "This is useful for avoiding race conditions between pipelines running "
            "in parallel."
        ),
    )

    def create_dir(self):
        self.path.mkdir(parents=True, exist_ok=True)

    def clean_dir(self):
        shutil.rmtree(self.path)

    @property
    def root_dir(self) -> Path:
        return Path(self.outputs_root_dir).joinpath(self.dir_prefix or "")

    @property
    def path(self) -> DagsterPath:
        return DagsterPath(Path(self.root_dir).joinpath(self.dir_name))

    @property
    def absolute_path(self) -> str:
        return str(self.path)

    def setup_for_execution(
		self, context: InitResourceContext
	) -> None:
        self.create_dir()

    def teardown_for_execution(
        self, context: InitResourceContext
    ) -> None:
        self.clean_dir()


class TemporaryDirectoryResource(BaseDirectoryResource):
    dir_name: str = "temp"
