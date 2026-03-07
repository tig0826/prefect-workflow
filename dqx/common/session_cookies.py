import json
import requests

from prefect.blocks.system import Secret

from common.login_dqx import login_dqx


# @task(name="save session cookies")
# def save_session_cookies(session):
#     cookies = session.cookies.get_dict()
#     cookie_block = JSON(value={"cookies": cookies})
#     cookie_block.save("dqx-session-cookies", overwrite=True)
#     print("Cookies saved:", cookies)


# @task(name="load session cookies")
# def load_session_cookies():
#     cookie_block = JSON.load("dqx-session-cookies")
#     cookies = cookie_block.value["cookies"]
#     # 新しいセッションにクッキーを適用
#     session = requests.Session()
#     for key, value in cookies.items():
#         session.cookies.set(key, value)
#     print("Cookies loaded:", session.cookies.get_dict())
#     return session


def save_session_cookies(session):
    cookies = session.cookies.get_dict()
    # dict → JSON 文字列化
    json_value = json.dumps({"cookies": cookies})
    # Secret ブロックとして保存
    secret_block = Secret(value=json_value)
    secret_block.save("dqx-session-cookies", overwrite=True)
    print("Cookies saved:", cookies)


def load_session_cookies():
    # Secretからデータを取得（文字列で来るか、辞書で来るかはPrefectの機嫌次第）
    secret_data = Secret.load("dqx-session-cookies").get()
    if isinstance(secret_data, str):
        # 文字列ならパースする
        parsed_data = json.loads(secret_data)
    elif isinstance(secret_data, dict):
        # すでに辞書ならそのまま使う
        parsed_data = secret_data
    else:
        raise ValueError(f"Unexpected type for secret data: {type(secret_data)}")
    cookies = parsed_data["cookies"]
    session = requests.Session()
    for key, value in cookies.items():
        session.cookies.set(key, value)
    print("Cookies loaded successfully.")
    return session


def login_dqx_and_save_cookies():
    session = login_dqx()  # 既存のログイン処理を呼び出し
    save_session_cookies(session)
    return session


def reuse_session():
    session = load_session_cookies()
    # セッションを利用して新しい操作を実行
    return session
