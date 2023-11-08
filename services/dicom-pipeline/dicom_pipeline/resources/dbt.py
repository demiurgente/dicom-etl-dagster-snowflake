from typing import List, Optional
from dagster_dbt import DbtCliResource
from dagster import OpExecutionContext


class DbtCli2(DbtCliResource):
    profiles_dir: str
    project_dir: str

    def cli(self, args: List[str],
        *,
        context: Optional[OpExecutionContext] = None):

        args = [*args, "--profiles-dir", self.profiles_dir, "--project-dir", self.project_dir]
        return super().cli(args=args, context=context)
