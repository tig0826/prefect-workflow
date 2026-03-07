import asyncio
from datetime import datetime, timezone

from prefect import flow, task, get_run_logger
from prefect.client.orchestration import get_client

from common.discord_notify import send_to_discord

COOKIE_SECRET_NAME = "dqx-session-cookies"
MAX_AGE_DAYS = 25


@task(name="fetch secret updated time")
def get_secret_updated_at(secret_name: str):
    # 内部に非同期処理を隔離する
    async def _fetch():
        async with get_client() as client:
            block_doc = await client.read_block_document_by_name(
                name=secret_name,
                block_type_slug="secret",
                include_secrets=False,
            )
            return block_doc.updated

    return asyncio.run(_fetch())


@flow(name="monitor dqx cookie secret age")
def notify_cookie_expiration():
    logger = get_run_logger()

    updated = get_secret_updated_at(COOKIE_SECRET_NAME)
    now = datetime.now(timezone.utc)
    age = now - updated
    age_days = age.days

    logger.info(
        f"Secret '{COOKIE_SECRET_NAME}' last updated at {updated.isoformat()}, "
        f"age={age_days} days"
    )

    if age_days >= MAX_AGE_DAYS:
        logger.warning(
            f"Secret '{COOKIE_SECRET_NAME}' age {age_days} >= {MAX_AGE_DAYS} days. "
            "Sending notification..."
        )
        message = f"<@1033018329360769095>\n⚠️ DQX cookie secret '{COOKIE_SECRET_NAME}' is {age_days} days old. Please update it soon!"
        webhook_url_name = "discord-webhook-url-test"
        send_to_discord(message, discord_webhook_url_name=webhook_url_name)
    else:
        logger.info(
            f"Secret '{COOKIE_SECRET_NAME}' is still fresh enough "
            f"({age_days} < {MAX_AGE_DAYS} days)."
        )
