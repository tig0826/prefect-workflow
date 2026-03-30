import math
import hashlib
import pandas as pd
import datetime
import googlemaps
from prefect.variables import Variable
from prefect.blocks.system import Secret
from common.trino_api import TrinoAPI


# --- 1. 空間距離計算（Haversine式） ---
def calculate_distance(lat1, lon1, lat2, lon2):
    """2つの緯度経度間の距離（メートル）を計算する"""
    R = 6371000  # 地球の半径 (メートル)
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# --- 2. テーブル初期化タスク ---
def init_tables(api: TrinoAPI):
    api.create_schema("life_silver")

    queries = [
        """
        CREATE TABLE IF NOT EXISTS iceberg.life_silver.dim_known_places (
            place_id VARCHAR,
            centroid_latitude DOUBLE,
            centroid_longitude DOUBLE,
            place_name VARCHAR,
            formatted_address VARCHAR,
            place_types VARCHAR, 
            created_at_jst TIMESTAMP(6) WITH TIME ZONE,
            updated_at_jst TIMESTAMP(6) WITH TIME ZONE
        ) WITH (format = 'PARQUET')
        """,
        """
        CREATE TABLE IF NOT EXISTS iceberg.life_silver.owntracks_stay_geocoding (
            stay_pk VARCHAR,
            place_id VARCHAR,
            is_api_called BOOLEAN,
            geocoded_at_jst TIMESTAMP(6) WITH TIME ZONE
        ) WITH (format = 'PARQUET', partitioning = ARRAY['day(geocoded_at_jst)'])
        """,
    ]
    for q in queries:
        api.execute_action(q)
    print("✅ テーブルの初期化完了")


# --- 3. 未処理データ＆マスタ抽出タスク ---
def fetch_data(api: TrinoAPI):
    query_unprocessed = """
        SELECT s.stay_pk, s.centroid_latitude, s.centroid_longitude
        FROM iceberg.life_silver.owntracks_stays s
        LEFT JOIN iceberg.life_silver.owntracks_stay_geocoding g ON s.stay_pk = g.stay_pk
        WHERE g.stay_pk IS NULL
    """
    df_stays = api.execute_query(query_unprocessed)

    # 🌟 修正: キャッシュヒット時に将来参照できるよう、全カラムを取得しておく
    query_master = """
        SELECT place_id, centroid_latitude, centroid_longitude, place_name, formatted_address, place_types
        FROM iceberg.life_silver.dim_known_places
    """
    df_master = api.execute_query(query_master)

    return df_stays, df_master


# --- 4. 判定とAPI呼び出しタスク ---
def process_geocoding(df_stays, df_master, api: TrinoAPI, gmaps_api_key: str):
    if df_stays.empty:
        print("✅ 未処理の滞在ログはありません。")
        return

    gmaps = googlemaps.Client(key=gmaps_api_key)

    new_places = []
    geocoding_results = []

    master_records = df_master.to_dict("records") if not df_master.empty else []
    now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))

    for _, row in df_stays.iterrows():
        stay_pk = row["stay_pk"]
        lat = float(row["centroid_latitude"])
        lon = float(row["centroid_longitude"])

        matched_place_id = None
        is_api_called = False

        # A. 空間キャッシュ引き当て（半径50m以内かチェック）
        for m in master_records:
            dist = calculate_distance(
                lat, lon, float(m["centroid_latitude"]), float(m["centroid_longitude"])
            )
            if dist <= 50.0:
                matched_place_id = m["place_id"]
                print(f"🎯 キャッシュヒット: {m['place_name']} (距離: {dist:.1f}m)")
                break

        # B. キャッシュミス: 未知の場所なら Google API を叩く
        if not matched_place_id:
            is_api_called = True
            print(f"🔍 未知の座標 ({lat}, {lon})。Google API を呼び出します...")

            new_place_id = hashlib.md5(f"{lat}_{lon}".encode("utf-8")).hexdigest()
            matched_place_id = new_place_id

            place_name = "Unknown Place"
            formatted_address = None
            place_types = None

            try:
                # 🌟 ハイブリッド・アプローチ: まずは施設を検索
                places_result = gmaps.places_nearby(
                    location=(lat, lon), radius=30, language="ja"
                )

                valid_place = None
                if places_result.get("status") == "OK":
                    for place in places_result["results"]:
                        types = place.get("types", [])
                        # 🚫 ゴミデータ（道路や行政区画）をブラックリストで弾く
                        invalid_types = {
                            "route",
                            "locality",
                            "political",
                            "sublocality",
                            "neighborhood",
                            "administrative_area_level_1",
                            "administrative_area_level_2",
                        }
                        if not invalid_types.intersection(set(types)):
                            valid_place = place
                            break

                if valid_place:
                    place_name = valid_place.get("name", "Unknown")
                    formatted_address = valid_place.get("vicinity", None)
                    place_types = ",".join(valid_place.get("types", []))
                    print(f"🏪 施設を発見: {place_name} ({formatted_address})")
                else:
                    # 🌟 施設がない、または道路しか返さなかった場合は Reverse Geocoding に強制フォールバック
                    reverse_results = gmaps.reverse_geocode((lat, lon), language="ja")
                    if reverse_results:
                        best_match = reverse_results[0]
                        raw_addr = best_match.get("formatted_address", "")

                        # 住所から建物名（末尾の文字列）を抽出するハック
                        normalized_addr = raw_addr.replace("　", " ")
                        addr_parts = normalized_addr.split(" ")

                        if len(addr_parts) >= 3:
                            place_name = addr_parts[-1]
                            formatted_address = addr_parts[1]
                        elif len(addr_parts) == 2:
                            place_name = addr_parts[1].split("市")[-1].split("区")[-1]
                            formatted_address = addr_parts[1]
                        else:
                            place_name = raw_addr
                            formatted_address = raw_addr

                        place_types = ",".join(best_match.get("types", []))
                        print(f"🏠 住所/建物を発見: {place_name} ({formatted_address})")
                    else:
                        print(
                            "⚠️ 何も見つかりませんでした。Unknown Placeとして登録します。"
                        )

            except Exception as e:
                print(f"❌ APIエラー: {e}")

            # 新規マスタ行の追加
            new_master_record = {
                "place_id": new_place_id,
                "centroid_latitude": lat,
                "centroid_longitude": lon,
                "place_name": place_name,
                "formatted_address": formatted_address,
                "place_types": place_types,
                "created_at_jst": now_jst,
                "updated_at_jst": now_jst,
            }
            new_places.append(new_master_record)
            master_records.append(new_master_record)

        # 滞在履歴への紐付け
        geocoding_results.append(
            {
                "stay_pk": stay_pk,
                "place_id": matched_place_id,
                "is_api_called": is_api_called,
                "geocoded_at_jst": now_jst,
            }
        )

    # --- 5. Trino (Iceberg) への INSERT ---
    if new_places:
        df_new_places = pd.DataFrame(new_places)
        api.insert_table("dim_known_places", "life_silver", df_new_places)
        print(f"📥 {len(new_places)} 件の新規場所をマスタに追加しました。")

    if geocoding_results:
        df_results = pd.DataFrame(geocoding_results)
        api.insert_table("owntracks_stay_geocoding", "life_silver", df_results)
        print(f"📥 {len(geocoding_results)} 件の滞在ログをエンリッチメントしました。")


# --- メインフロー ---
def enrich_places_flow():
    api = TrinoAPI(
        host=Variable.get("trino-host", default="trino.mynet"),
        port=80,
        user="tig",
        catalog="iceberg",
    )

    gmaps_api_key = Secret.load("google-maps-api-key").get()

    init_tables(api)
    df_stays, df_master = fetch_data(api)
    process_geocoding(df_stays, df_master, api, gmaps_api_key)


if __name__ == "__main__":
    enrich_places_flow()
