#!/usr/bin/env python3
import sys
from aw_client import ActivityWatchClient

def sync(src_url, dest_url):
    # 同期元 (Local)
    src = ActivityWatchClient("aw-sync-script", testing=False, host="localhost", port=5600)
    # クライアントのURLを強制的に書き換えるハック (ライブラリの仕様による)
    src.server_address = src_url

    # 同期先 (Remote)
    # URLパースが必要だが、簡易的に
    if "://" in dest_url:
        proto, rest = dest_url.split("://")
        host_port = rest.split("/")[0] # aw.mynet
        if ":" in host_port:
            host, port = host_port.split(":")
            port = int(port)
        else:
            host = host_port
            port = 80 if proto == "http" else 443
    
    dest = ActivityWatchClient("aw-sync-script", testing=False, host=host, port=port)

    # バケツ一覧取得
    buckets = src.get_buckets()
    
    for bucket_id in buckets:
        print(f"Syncing bucket: {bucket_id}...")
        
        # イベント取得 (全件は重いので、最新のものを...といきたいが、まずは全件取得)
        events = src.get_events(bucket_id, limit=-1)
        
        if not events:
            continue
            
        # リモートにバケツがなければ作成
        if bucket_id not in dest.get_buckets():
            bucket = buckets[bucket_id]
            dest.create_bucket(bucket_id, bucket["type"], bucket["hostname"], bucket["client"])
        
        # イベント送信 (重複は無視されるはず)
        dest.insert_events(bucket_id, events)
        print(f"  -> Sent {len(events)} events.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python sync.py <src> <dest>")
        sys.exit(1)
    
    sync(sys.argv[1], sys.argv[2])
