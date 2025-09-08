import requests
from prefect.blocks.system import Secret


def send_to_discord(message, image_path=None, discord_webhook_url_name=None):
    if discord_webhook_url_name:
        secret_block_webhook_url = Secret.load(discord_webhook_url_name)
    else:
        secret_block_webhook_url = Secret.load("discord-webhook-url")
    webhook_url = secret_block_webhook_url.get()
    if image_path:
        payload = {"content": message}
        files = {"image": open(image_path, "rb")}
        response = requests.post(webhook_url,
                                 data=payload,
                                 files=files)
    else:
        payload = {"content": message}
        response = requests.post(webhook_url,
                                 json=payload)
    if response.status_code == 204 or response.status_code == 200:
        print("メッセージが送信されました")
    else:
        print(f"エラーが発生しました: {response.status_code} - {response.text}")
