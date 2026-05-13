import sys
import os
from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from dbt_common.events.base_types import EventLevel

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DBT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


@flow(name="LifeOS Integrated Data Pipeline")
def main_data_flow():
    print("STEP 1: Building Silver & Intermediate layers...")
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
            log_level=EventLevel.INFO,
        )
    ).invoke(["build", "--select", "models/silver", "models/intermediate"])

    print("STEP 2: Building Gold layer...")
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
            log_level=EventLevel.INFO,
        )
    ).invoke(["build", "--select", "models/gold", "--vars", "reprocess_days: 7"])


if __name__ == "__main__":
    main_data_flow()
