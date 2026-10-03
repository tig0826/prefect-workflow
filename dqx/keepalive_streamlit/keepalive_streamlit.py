from prefect import flow, task, get_run_logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

APP_URL = "https://dqx-kishoukaku.streamlit.app/"


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def warm_streamlit(url: str):
    logger = get_run_logger()
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        page = browser.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        except PlaywrightTimeout as e:
            logger.error(f"Timeout navigating to app: {e}")
            raise

        try:
            # Streamlit apps waking from sleep can take well over 60s to render
            page.wait_for_selector("div[data-testid='stApp']", timeout=180_000)
            logger.info(f"Streamlit app loaded: {url}")
            # Brief scroll to simulate real user activity
            page.mouse.wheel(0, 300)
            page.wait_for_timeout(3000)
            page.mouse.wheel(0, -300)
            page.wait_for_timeout(2000)
            logger.info("Activity simulation complete")
        except PlaywrightTimeout:
            logger.warning("stApp not visible within timeout — app may have an error, but visit was recorded")
        finally:
            browser.close()


@flow(log_prints=True)
def keepalive_streamlit():
    warm_streamlit(APP_URL)
