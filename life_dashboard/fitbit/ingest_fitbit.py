import fitbit
import json
import os
import datetime

# === 設定 ===
CLIENT_ID = '23TLLK'         # 自分のIDに書き換えろ
CLIENT_SECRET = '61955cf9bbf82aa6a8f40c67a058e0d6' # 自分のSecretに書き換えろ
TOKEN_FILE = 'fitbit.json'           # さっき作ったファイル
OUTPUT_DIR = './data/raw/fitbit'     # データ保存先

def load_token():
    if not os.path.exists(TOKEN_FILE):
        raise FileNotFoundError(f"おい、{TOKEN_FILE} が見つからないぞ。初回認証はやったのか？")
    with open(TOKEN_FILE, 'r') as f:
        return json.load(f)

def save_token(token):
    # トークン更新時のコールバック
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token, f, indent=4)
    print(f"🔄 トークンを更新して {TOKEN_FILE} に保存したぞ。")

def main():
    # 1. トークン読み込み
    token = load_token()

    # 2. クライアント初期化 (ここを修正した！)
    # Fitbitクラスが全てのAPIメソッドを持っている
    try:
        authd_client = fitbit.Fitbit(
            CLIENT_ID,
            CLIENT_SECRET,
            access_token=token['access_token'],
            refresh_token=token['refresh_token'],
            expires_at=token['expires_at'],
            refresh_cb=save_token 
        )
    except TypeError:
        # 万が一古いライブラリで expires_at が非対応の場合の保険
        authd_client = fitbit.Fitbit(
            CLIENT_ID,
            CLIENT_SECRET,
            access_token=token['access_token'],
            refresh_token=token['refresh_token'],
            refresh_cb=save_token 
        )

    # 3. データ取得対象の日付
    target_date = datetime.date.today()# - datetime.timedelta(days=1)
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"📅 {date_str} のデータを取得中...")

    # 4. APIコール
    data_payload = {}
    
    try:
        # [A] アクティビティ (歩数、カロリーなど)
        # summaryエンドポイント
        print("...アクティビティ取得中")
        activity_res = authd_client.activities(date=target_date)
        data_payload['activities'] = activity_res
        
        # [B] 睡眠データ
        print("...睡眠データ取得中")
        sleep_res = authd_client.sleep(date=target_date)
        data_payload['sleep'] = sleep_res

        # [C] 体重・体脂肪
        print("...身体データ取得中")
        body_res = authd_client.body(date=target_date)
        data_payload['body'] = body_res
        
        # [D] 心拍数
        # print("...心拍数データ取得中")
        # heart_res = authd_client.heart(date=target_date)
        # data_payload['heart'] = heart_res
        # [D] 心拍数 (修正版: time_seriesメソッドを使用)
        print("...心拍数データ取得中")
        # 'activities/heart' というリソースパスを明示的に指定する
        heart_res = authd_client.time_series(
            resource='activities/heart', 
            base_date=target_date, 
            period='1d'
        )
        data_payload['heart'] = heart_res
    except Exception as e:
        print(f"❌ API呼び出し中にエラーだ: {e}")
        import traceback
        traceback.print_exc()
        return

    # 5. Rawデータ保存
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"{date_str}.json")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data_payload, f, ensure_ascii=False, indent=4)
    
    print(f"✅ 完了だ。データはここに捨てておいた: {output_file}")

if __name__ == '__main__':
    main()
