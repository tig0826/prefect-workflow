import os
from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from dbt_common.events.base_types import EventLevel

# プロジェクトディレクトリへの絶対パスを取得
DBT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


@flow(name="LifeOS dbt Build Flow", retries=1, retry_delay_seconds=30)
def run_lifeos_dbt_models(dbt_select: str = None):
    """dbt_lifeos のモデルを実行するPrefectフロー"""
    invoke_args = ["build"]
    if dbt_select:
        invoke_args.append("--select")
        invoke_args.extend(dbt_select.split())

    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
            log_level=EventLevel.INFO,
        )
    ).invoke(invoke_args)


if __name__ == "__main__":
    run_lifeos_dbt_models()
