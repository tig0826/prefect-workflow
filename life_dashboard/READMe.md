# Life Dashboard Data Pipeline

各種ソース（Fitbit, ActivityWatch, あすけん, Google タイムライン等）からライフログを収集し、
分析・可視化・AIフィードバック用に加工・提供するためのデータパイプラインです。

※ OwnTracks は 2026-05-06 で停止しており、現在どのマートからも参照されていない（実質廃止）。

## 🏗 アーキテクチャ

1.  **Exporters**: Python (Prefect) を使用し、各種APIやDBからデータを取得して S3 (Iceberg) に格納します。
2.  **Trino**: 全てのデータのクエリ・エンジンとして機能します。
3.  **dbt**: Trino 上でデータ変換を行い、Bronze (Raw) -> Silver (Cleaned) -> Gold (Mart) の順に加工します。
4.  **Gold Layer**: 可視化に最適化されたデータ（例：`mrt_behavior_slots_15m`）を `life_gold` スキーマに保持します。

## 📂 プロジェクト構成

- `asken_exporter/`: あすけん（食事・栄養）データ収集
- `aw_exporter/`: ActivityWatch（PC作業・AFK）データ収集
- `fitbit_exporter/`: Fitbit（睡眠・歩数・心拍数）データ収集
- `timeline_exporter/`: Google タイムライン（位置情報・外出）データ収集と地名解決
- `common/`: 共有タスク、Trino API連携
- `dbt_lifeos/`: dbt プロジェクト（モデル、マクロ、テスト）
  - `models/gold/`: ダッシュボード用マート（Gold層）
  - `models/intermediate/`: カテゴライズ・統合ロジック
  - `models/silver/`: 各ソースのクレンジング

## 🚀 実行方法

### 1. Prefect ワークフローの実行
個別のフローを実行してデータを収集します：
```bash
uv run python -m aw_exporter.aw_flow
uv run python -m fitbit_exporter.fitbit_flow
```

### 2. dbt モデルの更新
データを最新の状態に加工します：
```bash
cd dbt_lifeos
dbt run
```

## 📊 ダッシュボードとの連携
加工済みのデータは、別プロジェクト `life_dashboard_ui` (Next.js) から Trino 経由で参照されます。
`life_gold.mrt_behavior_slots_15m` がダッシュボードの行動タイムラインのメインデータソースです。

## 🤖 AI フィードバック（2026-08 再設計）

旧構成（朝8時/昼13時/夜22時の3回）は廃止。実測で3枠が同じ内容を再放送していた
（直近30日70件のうち81%が「深夜」に言及）ため、**日次1回 + 週次1回**に変更した。

| deployment | 実行 | 役割 |
|---|---|---|
| `life_ai_feedback_gate` | 05:00-11:00 の15分ごと | 起床を検知して日次FBを1回だけ生成。11時までに検知できなければ強制発火 |
| `life_ai_feedback_weekly` | 月曜 07:30 | **処方（今週の実験）を出す唯一の場所**。SQL探索権限と検索を持つ |
| `life_ai_feedback_daily_manual` | 手動 | 起床検知を待たずに再生成 |

- `ai_feedback/daily_flow.py` … **進行中 issue の実況**に徹する。処方は最大1つ。
  主対象は `awake_span`（前回の起床〜今朝の起床の連続区間）。暦日で切ると
  前日の夜〜深夜が集計の隙間に落ちるため。
- `ai_feedback/weekly_flow.py` … 洞察と「今週の実験」の決定。完全性クリティック付き。
- `ai_feedback/issue_tracker.py` … 状態管理。`ai_feedback_issues` / `ai_interventions` /
  `ai_metric_history` / `ai_instrumentation_proposals`。
- `ai_feedback/discovery.py` + `stats.py` … 構造的総当たり発見（重なり/遷移/不在）と BH-FDR。
- `ai_feedback/sql_tool.py` … 週次に渡す読み取り専用SQLツール。URL・曲名・食事の品目は参照不可。

**反復を防いでいる仕組み**: issue に `metric_sql`（1行1列の数値・`{eval_date}` プレースホルダ）を
必須にし、言及4回で metric が動かなければ自動で `abandoned` にする。
`metric_sql` を書けない処方（「十分な睡眠を」など）は `create_issue` が例外で弾く。

## ⚠️ 落とし穴（実際に踏んだもの）

### incremental_strategy は merge を安易に使わない
**ソースがスナップショットなら `delete+insert`、真の追記ログなら `merge`。**
`merge` は「消えた行」を削除できないため、スナップショット由来のテーブルでは孤児が残る。

実害:
- AW は**進行中イベントをポーリングごとに新しい `source_event_id`** で返す。
  merge だと15分ごとに1行ずつ孤児が積み上がり、**8日間で重なり515ペア・5,016分の二重計上**。
- あすけんで削除した食事が残り、品目合計2549kcal / 栄養1018kcal の矛盾。

対応済み: `aw_window_events` / `aw_afk_events` / `aw_media_events` / `aw_web_events` /
`aw_unlock_events`（`unique_key='source_dt'`）、`int_aw_categorized` / `int_aw_media` /
`int_aw_web`（`unique_key='event_date_jst'`）、`asken_meal`（`unique_key='meal_date'`）。

### 欠測を 0 で埋めない
`COALESCE(resting_heart_rate, 0)` で NULL を 0 に潰していたため、7日移動平均が
**78.5 → 68.6** に落ち、AI が存在しない急上昇を警告していた。`NULLIF` に変更し、
`resting_hr_7d_n`（何日分の平均か）を併記した。**欠測日数が違う週の平均を比較してはいけない。**

### current_date は UTC 基準
Trino のセッションTZが UTC なので、JST の早朝に1日ずれる（JST 08:00 = UTC 前日23:00）。
窓が7日以下のモデルでは当日を取り逃す。`CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS DATE)`
を使う。14日窓は1日のずれが無害なのでそのまま。

### incremental の cutoff は COALESCE する
`WHERE dt >= (SELECT MAX(...) FROM {{ this }})` は、テーブルが空だと NULL になり
`dt >= NULL` が常に偽で**永久に何も入らない**。
また WHERE 句内のサブクエリは `CROSS JOIN UNNEST` と組み合わせると Trino が
decorrelate できず落ちるので、CTE にして CROSS JOIN で持ち込む。

### Fitbit の睡眠は v1 と v1.2 で値が違う
`python-fitbit` の `client.sleep()` は v1（classic）で、**restless を睡眠に含むため
v1.2 より 14.9% 多い**（23セッションで4585分 vs 3991分）。段階の区間時系列
（`levels.data`）は v1.2 にしかないので `_get('/1.2/user/-/sleep/...')` で別途取得している
（`fitbit_sleep_stages`）。**v1 と v1.2 は同一セッションに別の logId を振る**ので
log_id で結合できない。突き合わせは時間の重なりで行う。

### 主睡眠/昼寝のラベルは逆転しうる
`cat_sub` は Fitbit の `isMainSleep` をそのまま使っており、Fitbit は**その日の最長
セッション**を主睡眠とする。昼寝が長いと逆転する（2026-08-27: 15:18-20:03の284分が
「主睡眠」、実際の夜間睡眠03:46-06:02の135分が「昼寝」）。
起床検知は**ラベルに依存せず**「60分以上のセッションが終わっていること」で判定する。

### 睡眠中計測の指標は MNAR
HRV / 呼吸数 / SpO2 / 皮膚温はカバレッジ55%前後で、**欠測日が完全に一致する**
（寝ていない夜・時計を外した夜にまとめて落ちる＝調子が悪い日に限って欠ける）。
`signals_available >= 3` の日だけ複合として評価し、**欠測を「改善」と読まない**。

### 位置情報は stays と routes で鮮度が違う
`mrt_location_stays` は `timeline_segments` の `visit` 由来、
`mrt_location_routes` は `timelinePath` 由来。Google が `visit` を返さない日は
stays が伸びないが**これは正常**（外出が記録されていないだけ）。
実測で 2026-08-25 以降は `timelinePath` のみで `visit` が 0 件。
なお `owntracks_*` / `location_stays_silver` は 2026-05-06 で停止しており、
**現在どのマートからも参照されていない**（OwnTracks 系は実質廃止）。

## 🔎 分析時の注意

- **`mrt_ai_activity_hourly` を source 横断で合算しない**（同じ行動が二重に数えられる）。
  画面時間の総量は `mrt_ai_screen_hourly`（分単位で重複排除済み）を使う。
- **`mrt_behavior_slots_15m` は分析に使わない**。1スロット1カテゴリで priority の勝者だけを
  残すため、睡眠が MEDIA を上書きして寝落ち視聴が見えない。UI 用。
- **`dev_score` の増加を単独で生産性向上と読まない**。同じ日に `work_score` が落ちていれば
  業務から個人開発への置き換えであり、総生産量の増加ではない。
  `dev_score` はアプリ前面時間ベースで、AIペアプロの待ち時間も計上される。
- **音楽は娯楽と分ける**。`cat_main='MUSIC'`（YouTube Music / Spotify / Amazon Music）は
  作業BGMなので、ダッシュボードの Leisure（`ENT_CATS = MEDIA/MANGA/GAME/SOCIAL`）から除外している。
