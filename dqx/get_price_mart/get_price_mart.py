from dbt_common.events.base_types import EventLevel
from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings


@flow(retries=3, retry_delay_seconds=60)
def get_price_mart():
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir="/opt/prefect/get_price_mart",
            profiles_dir="/opt/dbt/",
            log_level=EventLevel.ERROR,
        )
    ).invoke(["build"])


if __name__ == "__main__":
    get_price_mart()

