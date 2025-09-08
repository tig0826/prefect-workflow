from datetime import datetime
import requests
import os
from prefect import flow, get_run_logger
from prefect.tasks import task
from prefect.blocks.system import Secret

from common.discord_notify import send_to_discord

API_BASE_URL = "http://langflow-service-backend.langflow.svc.cluster.local:7860/api/v1"
FLOW_ID = "7082999c-18c0-4090-a471-c8cee554e757"

@task(retries=10, retry_delay_seconds=10, log_prints=True)
def call_langflow(api_key: str, input_text: str) -> str:
    logger = get_run_logger()
    url = f"{API_BASE_URL}/run/{FLOW_ID}"
    headers = {"Content-Type": "application/json", "x-api-key": api_key}
    payload = {"output_type": "chat", "input_type": "chat", "input_value": input_text}

    logger.info("LangflowにPOST開始: %s", url)
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=1200)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        # タイムアウト/HTTPエラー含めて詳細を出す
        logger.exception("Langflow呼び出しで例外: %s", e)
        raise

    # デバッグ用に短くボディを出す（長文全出しは避ける）
    body_preview = (resp.text[:800] + "...") if len(resp.text) > 800 else resp.text
    logger.debug("レスポンス（先頭800文字）: %s", body_preview)

    # パスが変わった時にも落ちないように安全に取り出す
    try:
        data = resp.json()
        text = (
            data["outputs"][0]["outputs"][0]["results"]["message"]["data"]["text"]
        )
    except Exception as e:
        logger.error("JSONの想定パスに値がありません。Raw JSONを確認してください。")
        logger.exception("パース失敗: %s", e)
        raise

    logger.info("Langflow応答（要約）: %s", (text[:200] + "...") if len(text) > 200 else text)
    return text

@flow(name="create-llm-message", log_prints=False)
def create_llm_message():
    logger = get_run_logger()
    api_key = Secret.load("langflow-api-key").get()
    current_date = datetime.today().strftime("%Y-%m-%d")
    prompt = (
        f"{current_date}の相場情報として、前日の21時からの相場について重要なポイントをまとめて報告してください。また、今後の相場の動きの予想もしながら冒険者(ユーザのこと)はどのような動きをとるべきかアドバイスも添えてください。"
    )
    result_text = call_langflow.submit(api_key, prompt).result()
    # 結果をINFOでログにも出す
    logger.info("最終結果:\n%s", result_text)
    send_to_discord(result_text, discord_webhook_url_name="discord-webhook-url-dogudora-news")
    return result_text

if __name__ == "__main__":
    create_llm_message()





