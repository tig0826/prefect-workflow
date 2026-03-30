# dbt_lifeos/main_data_flow.py
import sys
import os
from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from dbt_common.events.base_types import EventLevel

# パスを追加
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from enrich_places_flow import enrich_places_flow

DBT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


@flow(name="LifeOS Integrated Data Pipeline")
def main_data_flow():
    """
    データマート構築の全自動統合パイプライン
    """

    print("🚀 STEP 1: Building Silver & Intermediate layers...")
    # サブフローを使わず直接DbtRunnerを起動！
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
            log_level=EventLevel.INFO,
        )
    ).invoke(["build", "--select", "models/silver", "models/intermediate"])

    print("🚀 STEP 2: Running Places Geocoding Enrichment...")
    enrich_places_flow()

    print("🚀 STEP 3: Building Gold layer...")
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
            log_level=EventLevel.INFO,
        )
    ).invoke(["build", "--select", "models/gold"])


if __name__ == "__main__":
    main_data_flow()
