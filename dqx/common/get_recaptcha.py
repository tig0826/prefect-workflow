from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime, timedelta
from prefect.blocks.system import Secret

class get_recaptcha_token:
    def __init__(self, login_url):
        self.login_url = login_url

    def get_recaptcha_token(self):
        # Chromeドライバをwebdriver-managerで自動インストール
        options = webdriver.ChromeOptions()
        options.add_argument("--incognito")  # 必要に応じて設定
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        try:
            # reCAPTCHAのページにアクセス
            driver.get(self.login_url)
            # ユーザーが手動でreCAPTCHAを解くまで待機
            print("Please complete the reCAPTCHA in the browser window...")

            # `g-recaptcha-response`が存在し、値が設定されるのを待機
            WebDriverWait(driver, 300).until(
                lambda d: d.execute_script("return document.getElementById('g-recaptcha-response').value") != ''
            )
            # トークンの取得
            self.captcha_token = driver.execute_script(
                "return document.getElementById('g-recaptcha-response').value"
            )
            self.expiry_time = datetime.now() + timedelta(days=2)  # 仮に48時間有効とする
            self.save_secret("recaptcha-token", self.captcha_token)

        finally:
            # ブラウザを閉じる
            driver.quit()
        return self.captcha_token

    def save_secret(self, name, value):
        Secret(value=value).save(name=name, overwrite=True)

    def load_secret(self, name):
        return Secret.load(name).get()

if __name__ == "__main__":
    login_url = "https://secure.square-enix.com/oauth/oa/oauthlogin?client_id=dq_comm&response_type=code&svcgrp=Service_SEJ&retu=https%3A%2F%2Fhiroba.dqx.jp%2Fsc%2F&retl=dqx_p&redirect_uri=https%3A%2F%2Fsecure.dqx.jp%2Fsc%2Flogin%2Fexec%3Fp%3D0&facflg=1"
    recaptcha = get_recaptcha_token(login_url)
    recaptcha.get_recaptcha_token()
    print("Token:", recaptcha.load_secret("recaptcha-token"))
