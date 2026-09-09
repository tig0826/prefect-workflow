"""Google Health API のトークンを取得して Prefect Secret に保存する。

レガシーの Fitbit Web API は 2026年9月に完全停止する（月内の正確な日付は
Google 未公表）。体重・体脂肪はそれより先に死んでいて、2026-08-31 の実測を
最後に `/1/user/-/body/log/weight/...` へ新しいエントリが流れてこない。
アプリ側には値が入っているので、データが無いのではなく
レガシー API に配信されなくなったということ。

そのため取得経路を Google Health API (health.googleapis.com/v4) に移す。
このスクリプトは fitbit_exporter/get_fitbit_token.py と同じ役割で、
手元で一度だけ実行して Prefect Secret を作る。

前提（Google Cloud Console 側で先に済ませておくこと）:
  1. Google Health API を有効化
  2. Data Access ページで下記 SCOPES を登録
  3. OAuth 同意画面に自分のアカウントをテストユーザーとして追加
  4. 公開ステータスを「本番」にする
     ← Testing のままだと refresh token が7日で失効し、毎週パイプラインが
       止まる。未認証でも100ユーザーまでは本番運用できる（有償の
       第三者セキュリティ審査は100ユーザー超のときだけ必要）。

使い方:
    uv run python -m google_health_exporter.get_google_health_token \
        ~/Downloads/client_secret_XXX.apps.googleusercontent.com.json
"""

import json
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Optional

PREFECT_API = "http://prefect.mynet/api"

TOKEN_SECRET_NAME = "google-health-token"
CLIENT_ID_SECRET_NAME = "google-health-client-id"
CLIENT_SECRET_SECRET_NAME = "google-health-client-secret"

HEALTH_API_BASE = "https://health.googleapis.com/v4"

# 現行パイプラインが Fitbit から取っている項目を賄うのに必要な最小構成。
#   health_metrics_and_measurements … 体重 / 体脂肪
#   activity_and_fitness            … 歩数 / 心拍(1分刻み) / 安静時心拍 / カロリー
#   sleep                           … 睡眠と睡眠段階
# BMI は API に無いので身長と体重から自前で計算する。
SCOPES = [
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly",
]


# --------------------------------------------------------------------------
# HTTP ヘルパ（stdlib のみ。手元で一度動かすだけなので依存を増やさない）
# --------------------------------------------------------------------------
def _request(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[dict] = None,
    json_body: Optional[Any] = None,
    form_body: Optional[dict] = None,
) -> Any:
    data = None
    headers = dict(headers or {})
    if json_body is not None:
        data = json.dumps(json_body).encode()
        headers["Content-Type"] = "application/json"
    elif form_body is not None:
        data = urllib.parse.urlencode(form_body).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as res:
            raw = res.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"{method} {url} -> {e.code}\n{body}") from e


# --------------------------------------------------------------------------
# Prefect Secret ブロックの読み書き
# --------------------------------------------------------------------------
def _secret_schema_id(block_type_id: str) -> str:
    """新しい Secret ブロックに使う block schema を決める。

    このサーバには Secret のスキーマが6世代（Prefect 3.6.7〜3.6.20）
    同居していて、`block_schemas/filter` の先頭は最新版が返る。一方で
    実際に稼働中のフローが読めている `fitbit-token` は別世代を使っている。
    新規ブロックだけ最新世代にすると、ワーカー側の Prefect が古い場合に
    checksum 不一致で `Secret.load()` が落ちる。実績のある世代に合わせる。
    """
    docs = _request(
        f"{PREFECT_API}/block_documents/filter",
        method="POST",
        json_body={"block_documents": {"block_type_id": {"any_": [block_type_id]}}},
    )
    for reference in ("fitbit-token", "fitbit-client-id"):
        for d in docs or []:
            if d["name"] == reference:
                return d["block_schema_id"]

    schemas = _request(
        f"{PREFECT_API}/block_schemas/filter",
        method="POST",
        json_body={"block_schemas": {"block_type_id": {"any_": [block_type_id]}}},
    )
    if not schemas:
        raise RuntimeError("Secret の block schema が見つからない")
    return schemas[0]["id"]


def save_prefect_secret(name: str, value: str) -> None:
    """Secret ブロックを作成、既にあれば上書きする。

    手元に prefect CLI が入っていない前提なので REST API を直接叩く。
    """
    block_type = _request(f"{PREFECT_API}/block_types/slug/secret")
    schema_id = _secret_schema_id(block_type["id"])

    existing = _request(
        f"{PREFECT_API}/block_documents/filter",
        method="POST",
        json_body={"block_documents": {"name": {"any_": [name]}}},
    )
    if existing:
        _request(
            f"{PREFECT_API}/block_documents/{existing[0]['id']}",
            method="PATCH",
            json_body={"data": {"value": value}, "merge_existing_data": False},
        )
        print(f"  Secret '{name}' を上書きした")
    else:
        _request(
            f"{PREFECT_API}/block_documents/",
            method="POST",
            json_body={
                "name": name,
                "data": {"value": value},
                "block_schema_id": schema_id,
                "block_type_id": block_type["id"],
            },
        )
        print(f"  Secret '{name}' を作成した")


# --------------------------------------------------------------------------
# OAuth（ループバックフロー）
# --------------------------------------------------------------------------
class _CallbackHandler(BaseHTTPRequestHandler):
    code: Optional[str] = None
    error: Optional[str] = None

    def do_GET(self):  # noqa: N802
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        _CallbackHandler.code = query.get("code", [None])[0]
        _CallbackHandler.error = query.get("error", [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        msg = "認可できた。ターミナルに戻ってよい。" if _CallbackHandler.code else "認可に失敗した。"
        self.wfile.write(f"<html><body><h2>{msg}</h2></body></html>".encode())

    def log_message(self, *args):
        pass  # アクセスログは邪魔なので黙らせる


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def fetch_token(client_id: str, client_secret: str) -> dict:
    port = _free_port()
    redirect_uri = f"http://localhost:{port}"

    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        # refresh_token を必ず貰うために両方指定する。
        # prompt=consent を省くと二度目以降 refresh_token が返ってこない。
        "access_type": "offline",
        "prompt": "consent",
    }
    auth_url = "https://accounts.google.com/o/oauth2/auth?" + urllib.parse.urlencode(params)

    print("以下のURLをブラウザで開いて許可してください:\n")
    print("-" * 80)
    print(auth_url)
    print("-" * 80)
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    print(f"\nlocalhost:{port} で認可コードを待っています...")
    server = HTTPServer(("127.0.0.1", port), _CallbackHandler)
    server.handle_request()
    server.server_close()

    if _CallbackHandler.error:
        raise RuntimeError(f"認可を拒否された: {_CallbackHandler.error}")
    if not _CallbackHandler.code:
        raise RuntimeError("認可コードが取れなかった")

    print("トークンを交換中...")
    token = _request(
        "https://oauth2.googleapis.com/token",
        method="POST",
        form_body={
            "code": _CallbackHandler.code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
    )
    if "refresh_token" not in token:
        raise RuntimeError(
            "refresh_token が返ってこなかった。既存の認可を "
            "https://myaccount.google.com/permissions で解除してからやり直す。"
        )
    # expires_in は相対秒なので、絶対時刻に直して保存する。
    # Fitbit のトークン辞書と同じく expires_at を持たせておくと下流が楽。
    token["expires_at"] = (
        datetime.now(timezone.utc) + timedelta(seconds=token.get("expires_in", 3600))
    ).timestamp()
    return token


# --------------------------------------------------------------------------
# 疎通確認
# --------------------------------------------------------------------------
def smoke_test(access_token: str) -> None:
    """本当に9月分の体重が取れるのかをその場で確かめる。

    ここで値が返れば「アプリには入っているがレガシー API に来ない」という
    読みが裏付けられる。返らなければ Cloud Console 側の設定漏れか、
    Google Health にも同期されていないということになる。
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    lo = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    hi = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    # データ型はパスでは kebab-case、filter 式では snake_case。
    # 絞り込みに使える項目はデータ型ごとに違い、実測で確かめた結果はこう:
    #   単発計測(weight/body-fat) … sample_time.physical_time
    #   steps                     … interval.start_time
    #   sleep                     … interval.end_time のみ。start_time は
    #                               INVALID_DATA_POINT_FILTER_DATA_TYPE_MEMBER で弾かれる
    # (パス, filter 式の member, レスポンス JSON のキー)
    # JSON のキーは lowerCamelCase で、パスの kebab-case とは別物
    # （body-fat -> bodyFat）。機械的な変換では拾えないので明示する。
    targets = [
        ("weight", "weight.sample_time.physical_time", "weight"),
        ("body-fat", "body_fat.sample_time.physical_time", "bodyFat"),
        ("steps", "steps.interval.start_time", "steps"),
        ("sleep", "sleep.interval.end_time", "sleep"),
    ]
    for path_type, member, body_key in targets:
        expr = f'{member} >= "{lo}" AND {member} < "{hi}"'
        print(f"\n=== {path_type} (直近30日) ===")

        # 1ページでは返りきらない。pageSize を上げてもサーバ側で切られるので
        # nextPageToken を辿らないと件数を過小に見誤る（歩数を30日で42件と
        # 誤認したのはこれが原因）。
        points, token, pages = [], None, 0
        try:
            while True:
                query = {"filter": expr, "pageSize": 1000}
                if token:
                    query["pageToken"] = token
                res = _request(
                    f"{HEALTH_API_BASE}/users/me/dataTypes/{path_type}/dataPoints?"
                    + urllib.parse.urlencode(query),
                    headers=headers,
                )
                points.extend((res or {}).get("dataPoints") or [])
                token = (res or {}).get("nextPageToken")
                pages += 1
                if not token or pages >= 50:
                    break
        except RuntimeError as e:
            print(f"  取得失敗: {e}")
            continue

        print(f"  件数: {len(points)} ({pages}ページ)")

        # 同じ実測が複数のソースから重複して入る。実測では体重が
        # GOOGLE_WEB_API / HEALTH_CONNECT / FITBIT_WEB_API の3系統で
        # 最大3重になっていた。取り込み時は必ず潰す必要がある。
        platforms: dict[str, int] = {}
        for p in points:
            key = (p.get("dataSource") or {}).get("platform") or "?"
            platforms[key] = platforms.get(key, 0) + 1
        if platforms:
            print(f"  ソース別: {platforms}")

        body = points[0].get(body_key) if points else None
        if isinstance(body, dict) and "sampleTime" in body:
            uniq = {
                (
                    p[body_key]["sampleTime"]["physicalTime"],
                    json.dumps(
                        {k: v for k, v in p[body_key].items() if k != "sampleTime"},
                        sort_keys=True,
                    ),
                )
                for p in points
            }
            print(f"  重複除去後: {len(uniq)} 件")

        for p in points[:2]:
            print(f"  例: {json.dumps(p, ensure_ascii=False)[:300]}")


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    with open(sys.argv[1]) as f:
        conf = json.load(f)
    # デスクトップ用クライアントは "installed"、ウェブ用は "web" キーに入る
    creds = conf.get("installed") or conf.get("web")
    if not creds:
        raise RuntimeError("client secret JSON の形式が想定と違う")
    client_id = creds["client_id"]
    client_secret = creds["client_secret"]

    token = fetch_token(client_id, client_secret)

    # 公開ステータスが「テスト中」だと refresh token が7日で失効し、毎週
    # 手で同意を踏み直さないとパイプラインが黙って止まる。テスト中に発行された
    # トークンだけ refresh_token_expires_in が付いてくるので、それで判別する。
    ttl = token.get("refresh_token_expires_in")
    if ttl:
        print(
            f"\n⚠ この refresh token は約{round(ttl / 86400, 1)}日で失効する。"
            "OAuth 同意画面の公開ステータスが「テスト中」のままなので、"
            "「本番」に公開してからもう一度このスクリプトを実行すること。"
        )
    else:
        print("\n✅ refresh token に失効期限は付いていない（本番公開済みの状態で発行された）。")

    print("\nトークンを取得できた。Prefect Secret に保存する。")
    save_prefect_secret(CLIENT_ID_SECRET_NAME, client_id)
    save_prefect_secret(CLIENT_SECRET_SECRET_NAME, client_secret)
    save_prefect_secret(TOKEN_SECRET_NAME, json.dumps(token))

    print("\n--- 疎通確認 ---")
    smoke_test(token["access_token"])


if __name__ == "__main__":
    main()
