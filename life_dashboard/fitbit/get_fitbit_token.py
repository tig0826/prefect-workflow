import fitbit
from fitbit.api import FitbitOauth2Client
import json
import webbrowser
import os

# === ここにさっきのIDを入れる ===
CLIENT_ID = '23TLLK'
CLIENT_SECRET = '61955cf9bbf82aa6a8f40c67a058e0d6'
REDIRECT_URI = 'http://localhost:8080/'
# ==============================

def save_token(token):
    # トークンをJSONファイルとして保存
    with open('fitbit.json', 'w') as f:
        json.dump(token, f, indent=4)
    print("\n✅ トークンを 'fitbit.json' に保存しました！")
    print("このファイルを自宅サーバーのスクリプトと同じディレクトリに配置してください。")

def main():
    # OAuthクライアントの初期化
    server = FitbitOauth2Client(CLIENT_ID, CLIENT_SECRET)
    
    # 1. 認証用URLの生成
    # 必要な権限(Scope)を全て指定する
    url, _ = server.authorize_token_url(
        redirect_uri=REDIRECT_URI,
        scope=['activity', 'heartrate', 'location', 'nutrition', 'profile', 'settings', 'sleep', 'social', 'weight']
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

    # 2. コードの入力
    print("\nブラウザで許可した後、リダイレクトされたURL全体（http://localhost:8080/?code=...）をここに貼り付けてください。")
    redirected_url = input("Redirected URL: ").strip()
    
    # 3. コードの抽出とトークン交換
    # URLから 'code' パラメータを引っこ抜く
    from urllib.parse import urlparse, parse_qs
    query = parse_qs(urlparse(redirected_url).query)
    
    if 'code' not in query:
        print("❌ エラー: URLにコードが含まれていません。")
        return

    code = query['code'][0]
    
    # トークンを取得
    print("トークンを交換中...")
    try:
        server.fetch_access_token(code, REDIRECT_URI)
        token = server.session.token
        save_token(token)
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == '__main__':
    main()
