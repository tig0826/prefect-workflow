from fitbit.api import FitbitOauth2Client
import json
import webbrowser

from prefect.blocks.system import Secret

# === ここにさっきのIDを入れる ===

CLIENT_ID = Secret.load("fitbit-client-id").get()
CLIENT_SECRET = Secret.load("fitbit-client-secret").get()
REDIRECT_URI = "http://localhost:8080/"
# ==============================


def save_token(token: dict):
    """
    取得したFitbitトークン（辞書）をJSON文字列に変換し、
    PrefectのSecretブロック 'fitbit-token' に安全に保存（上書き）する。
    """
    try:
        token_str = json.dumps(token)
        Secret(value=token_str).save(name="fitbit-token", overwrite=True)
        print("\n✅ トークンを Prefect Secret 'fitbit-token' に安全に保存しました！")
    except Exception as e:
        print(f"\n❌ トークンの保存に失敗しました: {e}")


def main():
    # OAuthクライアントの初期化
    server = FitbitOauth2Client(CLIENT_ID, CLIENT_SECRET)
    # 必要な権限(Scope)を全て指定する。
    #
    # 2026-08-27 追加: 呼吸数・SpO2・皮膚温・心肺フィットネスは専用スコープが必要で、
    # 旧トークン（activity/heartrate/location/nutrition/profile/settings/sleep/social/weight）
    # では該当エンドポイントが 403 PERMISSION_DENIED になる。実測で確認済み。
    # なお HRV（/1/user/-/hrv/...）と AZM は heartrate/activity で取得できるため
    # 再認証しなくても使える。
    #
    # 注意: これらは基本的に睡眠中の計測なので、時計を外した夜・寝ていない夜は
    # 値が出ない（HRV は 60日中33日しか取れていない）。「欠測＝改善」と読まないこと。
    url, _ = server.authorize_token_url(
        redirect_uri=REDIRECT_URI,
        scope=[
            "activity",
            "cardio_fitness",
            "heartrate",
            "location",
            "nutrition",
            "oxygen_saturation",
            "profile",
            "respiratory_rate",
            "settings",
            "sleep",
            "social",
            "temperature",
            "weight",
        ],
    )

    print("以下のURLをブラウザで開いて、Fitbitへのアクセスを許可してください:\n")
    print("-" * 80)
    print(url)
    print("-" * 80)
    # 自動でブラウザを開く（環境によっては動かないのでprintもしている）
    try:
        webbrowser.open(url)
    except:
        pass
    print(
        "\nブラウザで許可した後、リダイレクトされたURL全体（http://localhost:8080/?code=...）をここに貼り付けてください。"
    )
    redirected_url = input("Redirected URL: ").strip()
    from urllib.parse import urlparse, parse_qs

    query = parse_qs(urlparse(redirected_url).query)
    if "code" not in query:
        print("❌ エラー: URLにコードが含まれていません。")
        return
    code = query["code"][0]
    # トークンを取得
    print("トークンを交換中...")
    try:
        server.fetch_access_token(code, REDIRECT_URI)
        token = server.session.token
        save_token(token)
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")


if __name__ == "__main__":
    main()
