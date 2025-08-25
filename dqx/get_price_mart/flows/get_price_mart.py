# flows/etl_flow.py
from prefect import flow, task
from prefect_dbt.cli.commands import DbtCoreOperation

@task
def scrape():
    # ここで raw.price_hourly を更新する
    return "done"

@flow
def etl_flow():
    scrape()
    DbtCoreOperation(
        commands=["dbt build --select state:modified+"],  # or "dbt build"
        project_dir="dbt_project",                        # dbt_project.yml がある場所
        profiles_dir="dbt_project/infra",                 # ← ここが肝（~/.dbt を使わない）
    )()

if __name__ == "__main__":
    etl_flow()
