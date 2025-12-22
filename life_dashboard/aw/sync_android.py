import sys
import subprocess
import time
import requests
from aw_client import ActivityWatchClient

# --- 設定 (自分の環境に合わせて書き換えろ) ---
TARGET_URL = "http://aw.mynet"   # 自宅サーバーのURL (または http://192.168.x.x:5600)
LOCAL_API = "http://127.0.0.1:5600"
# ----------------------------------------

def run_priv(cmd):
    """Shizuku (rish) 経由で特権コマンドを実行"""
    # rish -c 'コマンド' の形式で実行
    full_cmd = f"rish -c '{cmd}'"
    try:
        # シェルの実行結果を文字列で受け取る
        result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"⚠️ Command failed: {e}")
        return ""

def is_safe_to_sync():
    """
    【判定ロジック】
    画面が点いているか (Screen ON) を判定する。
    点いていればユーザーが操作中なので中止する。
    """
    # dumpsys window policy は画面の状態を詳しく教えてくれる
    output = run_priv("dumpsys window policy")
    
    # Androidのバージョンによって出力が違うので、複数のキーワードで網を張る
    # mScreenOnEarly=true: 画面が点灯準備完了
    # mAwake=true: システムが起きている
    # mWakefulness=Awake: 起きている
    if "mScreenOnEarly=true" in output or "mAwake=true" in output or "mWakefulness=Awake" in output:
        print("✋ Screen is ON. User is active. Skipping.")
        return False
        
    # 念の為 dumpsys power も確認 (二重チェック)
    output_p = run_priv("dumpsys power")
    if "mWakefulness=Awake" in output_p:
         print("✋ Screen is ON (Power Manager). Skipping.")
         return False

    return True

def sync():
    print("🔍 Checking device state...")
    
    # 1. ユーザーの邪魔をしないかチェック
    if not is_safe_to_sync():
        sys.exit(0)

    print("🥷 Ninja Mode: Screen is OFF. Executing Sync...")

    # 2. ActivityWatchアプリを裏で叩き起こす
    # 画面がOFFでも、このコマンドなら強制的にActivityを起動できる
    run_priv("am start -n net.activitywatch.android/net.activitywatch.android.MainActivity")
    
    # アプリが立ち上がってAPIサーバー(5600ポート)が開くまで少し待つ
    print("⏳ Waiting for App to launch...")
    time.sleep(10) 

    # 3. データ同期処理
    try:
        # ローカル(スマホ)のクライアント
        src = ActivityWatchClient("ninja-sync", testing=False, host="127.0.0.1", port=5600)
        # リモート(自宅サーバー)のクライアント
        dest = ActivityWatchClient("ninja-sync", testing=False)
        dest.server_address = TARGET_URL
        
        # バケツ(データ容器)の一覧を取得
        buckets = src.get_buckets()
        
        for bucket_id in buckets:
            # Android関連のバケツだけを送る
            if "android" not in bucket_id: continue
            
            print(f"📦 Processing bucket: {bucket_id}")
            
            # リモート側にバケツがなければ作る
            if bucket_id not in dest.get_buckets():
                b = buckets[bucket_id]
                dest.create_bucket(bucket_id, b["type"], b["hostname"], b["client"])
            
            # イベントを取得して送信
            # 一度に送る量が多いと失敗するので limit=500 くらいで刻むのが無難だが、
            # ここではシンプルに直近のデータを送る
            events = src.get_events(bucket_id, limit=1000)
            if events:
                dest.insert_events(bucket_id, events)
                print(f"  -> Synced {len(events)} events.")
            else:
                print("  -> No events found.")

        print("✅ Sync Complete.")

    except Exception as e:
        print(f"🔥 Sync Error: {e}")
        # エラーでもアプリは終了させる

    finally:
        # 4. 証拠隠滅 (アプリ終了)
        # メモリ節約のため、用が済んだら殺す
        print("👋 Killing ActivityWatch app...")
        run_priv("am force-stop net.activitywatch.android")

if __name__ == "__main__":
    sync()
