from bs4 import BeautifulSoup
import requests
from prefect import task
from prefect.blocks.system import Secret

from common.get_recaptcha import get_recaptcha_token


@task(name="login dqx", retries=5, retry_delay_seconds=5)
def login_dqx():
    # セッションを開始
    session = requests.Session()
    # ユーザーエージェントの設定
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
# ログインURL（フォームのaction属性から取得）
    base_url = 'https://secure.square-enix.com/oauth/oa/'
    login_endpoint = 'oauthlogin.send'
    query_params = {
        'client_id': 'dq_comm',
        'response_type': 'code',
        'svcgrp': 'Service_SEJ',
        'retl': 'dqx_p',
        'redirect_uri': 'https://secure.dqx.jp/sc/login/exec?p=0',
        'alar': '1'
    }

# reCAPTCHAトークンを取得
    recaptcha_url = "https://secure.square-enix.com/oauth/oa/oauthlogin?client_id=dq_comm&response_type=code&svcgrp=Service_SEJ&retu=https%3A%2F%2Fhiroba.dqx.jp%2Fsc%2F&retl=dqx_p&redirect_uri=https%3A%2F%2Fsecure.dqx.jp%2Fsc%2Flogin%2Fexec%3Fp%3D0&facflg=1"
    recaptcha = get_recaptcha_token(recaptcha_url)
    recaptcha_token = recaptcha.get_recaptcha_token()
    # recaptcha_token = Secret.load("recaptcha-token").get()

# 完全なログインURLを構築
    login_url = f"{base_url}{login_endpoint}"
# prefectからユーザIDとパスワードを取得
    secret_block_user = Secret.load("dqx-user")
    dqx_user = secret_block_user.get()
    secret_block_passwd = Secret.load("dqx-password")
    dqx_passwd = secret_block_passwd.get()
# ログインに必要なデータを辞書に格納
    login_data = {
        '_STORED_': 'dcf06e5f47798101ac74fb0e35135d9b5f83fa22842f00c9fcc43d0b31208e93909fda8298dff852ccd860bf72939f95528245f4a3916c2bda4e5008c818e522bed0c37d51ffa715c4524f04617eaafa5997e20b6ec0afe893a5b572358cfadc840d7f2f93cba8c9cdf40e3bd7237113e6',
        'sqexid': dqx_user,
        'password': dqx_passwd,
        'saveSqexid': '1',  # このフィールドはIDを記憶するかどうかの設定です。
        'wfp': '1',
        "g-recaptcha-response": recaptcha_token
    }
# ログインリクエストを送信
    response = session.post(login_url, data=login_data, headers=headers, params=query_params)
# レスポンスを確認
    if response.ok:
        print("Login successful!")
        # ログイン後の処理をここで行います。
    else:
        print("Login failed with status code: ", response.status_code)
# レスポンスからcis_session idを取得
    soup = BeautifulSoup(response.content, 'html.parser')
    print(soup)
    cis_sessid = soup.find('input', {'name': 'cis_sessid'})['value']

# JavaScriptで実行されるリダイレクト部分を実行
# 2つ目のリクエストのURL
    next_url = 'https://secure.dqx.jp/sc/login/exec?p=0'

# フォームから取得したデータ
    form_data = {
        'cis_sessid': cis_sessid,
        'provision': '',
        '_c': '1'
    }
# 2つ目のリクエストを送信
    next_response = session.post(next_url, data=form_data)
# 次のレスポンスの確認
    if next_response.ok:
        # 2つ目のリクエストに成功した場合の処理をここに書きます
        # 例えば、取得したい情報がある場合はそのページの内容をパースするなど
        data = next_response.text
        # 必要な情報を抽出する処理をここに書きます
    else:
        print("The second request failed with status code: ", next_response.status_code)

# キャラクターを選択する
    char_select_url = 'https://hiroba.dqx.jp/sc/login/characterexec'
# 選択したいキャラクターのID（rel属性の値）
    selected_char_id = '484618740227'  # 例えば、最初のキャラクターを選択する場合

# キャラクター選択に必要なデータを辞書に格納
    char_select_data = {
        'cid': selected_char_id,
        'aurl': '',  # 追加で必要な値があるか確認する
        'murl': ''   # 追加で必要な値があるか確認する
    }
# キャラクター選択リクエストを送信
    char_select_response = session.post(char_select_url, data=char_select_data)
# キャラクター選択後のレスポンスを確認
    if char_select_response.ok:
        # ページの内容を取得
        char_select_content = char_select_response.text
# キャラクター選択が成功したか、必要な情報を抽出する処理をここに書きます
    else:
        print("Failed to select character with status code:", char_select_response.status_code)

    return session
