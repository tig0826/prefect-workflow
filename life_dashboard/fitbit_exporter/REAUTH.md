# Fitbit の再認証手順（スコープ追加）

## なぜやるのか

`fitbit_scraper.py` が取得していなかった指標のうち、以下は**専用スコープが必要**で、
現行トークンでは `403 PERMISSION_DENIED` になる（2026-08-27 実測）。

| 指標 | エンドポイント | 必要スコープ |
|---|---|---|
| 呼吸数 | `/1/user/-/br/date/{date}.json` | `respiratory_rate` |
| SpO2 | `/1/user/-/spo2/date/{date}.json` | `oxygen_saturation` |
| 皮膚温 | `/1/user/-/temp/skin/date/{date}.json` | `temperature` |
| 心肺フィットネス(VO2max) | `/1/user/-/cardioscore/date/{date}.json` | `cardio_fitness` |

デバイス（Google Pixel Watch 3）は `EDA` / `SPO2` /
`NIGHTTIME_OXYGEN_SATURATION` を機能として持っているので、ハード側の制約ではない。

**HRV と AZM は再認証不要**（既存の `heartrate` / `activity` で取得できることを実測確認済み）。
先にそちらを実装する。

## 手順

ブラウザでの同意操作が1回必要なので、これは手で実行する。

```bash
cd ~/workspace/home_server/prefect-workflow/life_dashboard
.venv/bin/python -m fitbit_exporter.get_fitbit_token
```

1. 表示された URL がブラウザで開く（開かなければ手でコピーして開く）
2. Fitbit の同意画面で **すべての項目にチェック**を入れて許可する
   - 特に「呼吸数」「血中酸素ウェルネス」「皮膚温」「心肺機能フィットネススコア」
   - ここでチェックを外すと該当スコープが降りず、403 のままになる
3. `http://localhost:8080/?code=...` にリダイレクトされる
   （localhost で何も待ち受けていないのでブラウザはエラー表示になるが問題ない）
4. **アドレスバーの URL 全体**をコピーしてスクリプトのプロンプトに貼り付ける
5. `✅ トークンを Prefect Secret 'fitbit-token' に安全に保存しました！` が出れば完了

## 確認

```bash
cd ~/workspace/home_server/prefect-workflow/life_dashboard
.venv/bin/python - <<'EOF'
import json, urllib.request, urllib.error
from prefect.blocks.system import Secret
tok = Secret.load("fitbit-token").get()
if isinstance(tok, str): tok = json.loads(tok)
print("scope:", tok.get("scope"))
D = "2026-08-26"   # 実際に計測がある日に変える
for name, p in {
    "呼吸数": f"/1/user/-/br/date/{D}.json",
    "SpO2": f"/1/user/-/spo2/date/{D}.json",
    "皮膚温": f"/1/user/-/temp/skin/date/{D}.json",
    "心肺": f"/1/user/-/cardioscore/date/{D}.json",
}.items():
    r = urllib.request.Request(f"https://api.fitbit.com{p}",
                               headers={"Authorization": f"Bearer {tok['access_token']}"})
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            print(f"{name}: 200 {json.dumps(json.load(resp), ensure_ascii=False)[:160]}")
    except urllib.error.HTTPError as e:
        print(f"{name}: {e.code}")
EOF
```

`scope` に4つが含まれ、すべて 200 が返れば成功。

## 注意点

- **トークンは上書きされる。** 失敗しても `fitbit_flow` は refresh_cb で自走するが、
  途中で中断した場合は再度このスクリプトを完走させること。
- **欠測は避けられない。** これらは睡眠中の計測なので、時計を外した夜・寝ていない夜は
  値が出ない。HRV は 60日中33日、しかも 8/21（睡眠0h）や 8/23（睡眠3.9h）といった
  「悪い夜」に限って欠測している。**欠測を「改善」と読まないガードが下流で必須。**
- スコープを増やすと検定の分母（多重検定の対象）が増える。取れるようになっても、
  「調子の悪さを追えるか」の検証（Task #19）を通ったものだけを FB の根拠に使う。
