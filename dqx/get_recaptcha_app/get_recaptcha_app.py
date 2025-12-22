from flask import Flask, jsonify
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait

app = Flask(__name__)

# ログインページURL
LOGIN_URL = "https://secure.square-enix.com/oauth/oa/oauthlogin?client_id=dq_comm&response_type=code&svcgrp=Service_SEJ&retu=https%3A%2F%2Fhiroba.dqx.jp%2Fsc%2F&retl=dqx_p&redirect_uri=https%3A%2F%2Fsecure.dqx.jp%2Fsc%2Flogin%2Fexec%3Fp%3D0&facflg=1"

@app.route("/get-token", methods=["GET"])
def get_recaptcha_token():
    # Seleniumのセットアップ
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")  # 必要に応じてコメントアウト
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--window-size=1920,1080")  # デフォルトの画面サイズを設定

    driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=options)

    try:
        # ページを開く
        driver.get(LOGIN_URL)
        print("Please solve the reCAPTCHA manually...")

        # ユーザーがreCAPTCHAを解決するまで待機
        WebDriverWait(driver, 300).until(
            lambda d: d.execute_script("return document.getElementById('g-recaptcha-response').value") != ''
        )

        # トークン取得
        recaptcha_token = driver.execute_script("return document.getElementById('g-recaptcha-response').value")
        print("reCAPTCHA token:", recaptcha_token)

        # トークンを返す
        return jsonify({"recaptcha_token": recaptcha_token})

    finally:
        driver.quit()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
