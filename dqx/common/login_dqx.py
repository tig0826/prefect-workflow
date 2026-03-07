import subprocess
import re

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from prefect import task
from prefect.blocks.system import Secret
import time
import random


def get_chrome_major_version():
    """OSにインストールされているChromeのメジャーバージョンを動的に取得する"""
    try:
        # Linuxコンテナ環境用 (google-chrome)
        process = subprocess.run(
            ["google-chrome", "--version"], capture_output=True, text=True
        )
        version_text = process.stdout
        if not version_text:
            process = subprocess.run(
                [
                    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                    "--version",
                ],
                capture_output=True,
                text=True,
            )
            version_text = process.stdout
        # "Google Chrome 146.0.7632.160" のような文字列からメジャーバージョンを抽出
        match = re.search(r"(?:Chrome/|Google Chrome )(\d+)", version_text)
        if match:
            major_version = int(match.group(1))
            print(f"Detected Chrome major version: {major_version}")
            return major_version
    except Exception as e:
        print(f"Failed to detect Chrome version dynamically: {e}")
    return None


@task(name="login dqx", retries=5, retry_delay_seconds=5)
def login_dqx():
    # クレデンシャルの取得
    dqx_user = Secret.load("dqx-user").get()
    dqx_passwd = Secret.load("dqx-password").get()
    selected_char_id = Secret.load("dqx-character-id").get()
    # ---------------------------------------------------------
    # 1. undetected_chromedriver のセットアップ（ステルス化）
    # ---------------------------------------------------------
    options = uc.ChromeOptions()
    # ヘッドレスモード（K8s上で動かすための必須設定）
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # ウィンドウサイズを指定して、よりリアルなPC環境に偽装
    options.add_argument("--window-size=1920,1080")

    # バージョン不整合で落ちる場合は `version_main=120` など環境に合わせて指定しろ
    # driver = uc.Chrome(options=options)
    chrome_version = get_chrome_major_version()
    if chrome_version:
        driver = uc.Chrome(options=options, version_main=chrome_version)
    else:
        driver = uc.Chrome(options=options)
    session = requests.Session()
    # ucが自動で設定したリアルなUser-Agentを引っこ抜いて使い回す
    user_agent = driver.execute_script("return navigator.userAgent;")
    session.headers.update({"User-Agent": user_agent})

    try:
        # ---------------------------------------------------------
        # 2. ログインページへアクセス
        # ---------------------------------------------------------
        login_url = "https://secure.square-enix.com/oauth/oa/oauthlogin?client_id=dq_comm&response_type=code&svcgrp=Service_SEJ&retu=https%3A%2F%2Fhiroba.dqx.jp%2Fsc%2F&retl=dqx_p&redirect_uri=https%3A%2F%2Fsecure.dqx.jp%2Fsc%2Flogin%2Fexec%3Fp%3D0&facflg=1"
        driver.get(login_url)

        # ---------------------------------------------------------
        # 3. 人間らしい入力操作（ゆらぎの演出）
        # ---------------------------------------------------------
        print("Page loaded. Emulating human behavior...")
        time.sleep(
            random.uniform(2.0, 4.0)
        )  # ページ読み込み後、人間がフォームを認識するまでの間

        sqexid_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "sqexid"))
        )

        # IDを1文字ずつ、ランダムな間隔でタイピングする
        for char in dqx_user:
            sqexid_input.send_keys(char)
            time.sleep(random.uniform(0.05, 0.2))

        time.sleep(random.uniform(0.5, 1.5))  # 次のフィールドへ移動する間

        pw_input = driver.find_element(By.ID, "password")
        # パスワードも同様に1文字ずつタイピングする
        for char in dqx_passwd:
            pw_input.send_keys(char)
            time.sleep(random.uniform(0.05, 0.2))

        time.sleep(random.uniform(1.0, 2.5))  # ログインボタンを押すか迷う時間

        # ---------------------------------------------------------
        # 4. ログインボタンのクリック
        # ---------------------------------------------------------
        driver.find_element(By.ID, "login-button").click()

        # ---------------------------------------------------------
        # 5. ログイン完了を待機し、Cookieを強奪
        # ---------------------------------------------------------
        print("Waiting for login authorization and redirect...")

        # lambdaを使って、現在のURLにどちらかの文字列が含まれるまで待機させる
        WebDriverWait(driver, 45).until(
            lambda d: (
                "login/exec" in d.current_url or "hiroba.dqx.jp/sc/" in d.current_url
            )
        )
        time.sleep(3)  # Cookieがブラウザに完全にセットされるのを待つ

        for cookie in driver.get_cookies():
            session.cookies.set(cookie["name"], cookie["value"])

        print(
            "Login successful via undetected_chromedriver. Cookies extracted."
        )  # ---------------------------------------------------------

    except Exception as e:
        print(f"Selenium Login Failed: {e}")
        print(f"Current URL at failure: {driver.current_url}")
        # ★デバッグ用：エラー時の画面状態を画像として保存する★
        driver.save_screenshot("/tmp/dqx_login_error.png")
        print("Saved screenshot to /tmp/dqx_login_error.png")
        raise
    finally:
        driver.quit()

    # ---------------------------------------------------------
    # 6. キャラクター選択（以降は requests）
    # ---------------------------------------------------------
    char_select_url = "https://hiroba.dqx.jp/sc/login/characterexec"
    char_select_data = {"cid": selected_char_id, "aurl": "", "murl": ""}

    char_select_response = session.post(char_select_url, data=char_select_data)

    if char_select_response.ok:
        print("Character selected successfully. Session is ready for scraping.")
    else:
        print(
            "Failed to select character with status code:",
            char_select_response.status_code,
        )
        raise Exception("Character selection POST failed.")

    return session
