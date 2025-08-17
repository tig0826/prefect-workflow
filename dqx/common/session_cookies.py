from prefect.blocks.system import JSON
from prefect import flow, task
import requests

from common.login_dqx import login_dqx


@task(name="save session cookies")
def save_session_cookies(session):
    cookies = session.cookies.get_dict()
    cookie_block = JSON(value={"cookies": cookies})
    cookie_block.save("dqx-session-cookies", overwrite=True)
    print("Cookies saved:", cookies)


@task(name="load session cookies")
def load_session_cookies():
    cookie_block = JSON.load("dqx-session-cookies")
    cookies = cookie_block.value["cookies"]
    # 新しいセッションにクッキーを適用
    session = requests.Session()
    for key, value in cookies.items():
        session.cookies.set(key, value)
    print("Cookies loaded:", session.cookies.get_dict())
    return session


@task(name="login and save cookies", retries=5, retry_delay_seconds=5)
def login_dqx_and_save_cookies():
    session = login_dqx()  # 既存のログイン処理を呼び出し
    save_session_cookies(session)
    return session


@task(name="reuse session from cookies", retries=5, retry_delay_seconds=5)
def reuse_session():
    session = load_session_cookies()
    # セッションを利用して新しい操作を実行
    return session
