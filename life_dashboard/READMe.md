# Life Dashboard Data Pipeline

各種ソース（Fitbit, Google Health, ActivityWatch, あすけん, Google タイムライン等）から
ライフログを収集し、分析・可視化・AIフィードバック用に加工・提供するためのデータパイプラインです。

※ OwnTracks は 2026-05-06 で停止しており、現在どのマートからも参照されていない（実質廃止）。

※ **レガシー Fitbit Web API は 2026-09 に停止する。**体重・体脂肪は先に配信が止まり、
移行先は Google Health API。詳細は「Fitbit Web API の停止と Google Health への移行」を参照。

## 🏗 アーキテクチャ

1.  **Exporters**: Python (Prefect) を使用し、各種APIやDBからデータを取得して S3 (Iceberg) に格納します。
2.  **Trino**: 全てのデータのクエリ・エンジンとして機能します。
3.  **dbt**: Trino 上でデータ変換を行い、Bronze (Raw) -> Silver (Cleaned) -> Gold (Mart) の順に加工します。
4.  **Gold Layer**: 可視化に最適化されたデータ（例：`mrt_behavior_slots_15m`）を `life_gold` スキーマに保持します。

## 📂 プロジェクト構成

- `asken_exporter/`: あすけん（食事・栄養）データ収集
- `aw_exporter/`: ActivityWatch（PC作業・AFK）データ収集
- `fitbit_exporter/`: Fitbit（睡眠・歩数・心拍数）データ収集。**2026-09 に停止する旧API**
- `google_health_exporter/`: Google Health（体重・体脂肪・歩数・睡眠）データ収集。Fitbit の後継
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
uv run python -m google_health_exporter.google_health_flow
```

Google Health のトークン取得は手元で一度だけ（ブラウザでの同意が必要）:
```bash
uv run python -m google_health_exporter.get_google_health_token <client_secret.json>
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

### チケットの3層構造

GitHub が唯一の正で、Trino には載せない（出どころが2つになると「間違った方を読む」事故が起きる）。
リポジトリは `GITHUB_TICKET_REPO`（既定 `tig0826/life_dashboard_ui`）。

| ラベル | 層 | 意味 |
|---|---|---|
| `life:problem` | 課題 | 解決したい実体。`sev:S1`〜`S4` / `pri:P0`〜`P3` 付き |
| `life:hypothesis` | 仮説 | 課題の原因についての理論。`metric_sql` を持つ |
| `life:action` | 打った手 | 実験そのもの（DNSブロック等）。`kind:*` 付き |
| `life:instrumentation` | 計測できていない | 測る手段が無くて検証できない論点 |

**REST の `GET /issues/N` は `parent` を null で返す**（sub-issue 関係があっても）。
親は課題側の `sub_issues` を辿って逆引きする（`github_tickets.parent_map()`）。

### 仮説の状態遷移（2026-09-11 改修）

```
weekly（月曜）が起票 → open
   ├ 言及4回 + 介入あり + 7日経過 + 動かない → abandoned（反証）
   ├ 言及4回 + 介入なし                      → untested（未検証・介入が付けば復帰）
   └ 目標を5日連続で達成                      → verifying（検証完了・実況から外す）
```

**`verifying` は「課題が解決した」ではない。** 示されたのは打った手が効いたことだけで、
親の課題が動いたかは別問題。実例: #7「漫画アプリへの勤務中の逃避」の親は
#21「娯楽への逃避が、介入を重ねても総量として減らない」であり、漫画が減っても
**逃避先が移っただけの可能性**が残る。達成時に問いを
「この指標が下がったか」→「親の課題が動いたか」に切り替える。

改修前は `enforce_abandonment` が `is_moving` を素通りさせていたため、
**効いている仮説ほど死なず永久に毎朝実況されていた**
（実測: 18日中13日が漫画ブロックの話）。

### 毎日同じ話題になるのを防いでいる仕組み

2026-09-11 の実測で、18日間の話題分布が
漫画72% / 睡眠66% / 解錠61% / 業務50% に対し **体重0% / 歩数0% / 食事5%** だった。
原因は3つあり、いずれも修正済み。

1. **成功に出口が無かった** → 上記の `verifying`
2. **新鮮さの原則が書き換えで消えていた。** さらに `recent_feedback_history` が
   daily の経路では None で、**モデルは自分が昨日何を言ったかを知らずに書いていた**。
   両方復活（実況の枠だけは対象外。継続性が価値なので繰り返してよい）
3. **プロンプトの例示に引っ張られていた。** 「`phone_signals` の解錠回数など」と
   書いていたため解錠の話が61%を占めた。**例示は全削除**し、
   何を話題にするかはデータ側で計算して渡す:
   - `under_covered_domains` … 直近のFBで触れていない領域（枠の名前だけ。中身の例は持たない）
   - `metric_deviations` … 直近3日が過去28日の中央値から2割以上外れた指標
   - `review_candidates` … 客観指標で閉じられない課題（本人に問い直す対象）

**★プロンプトに具体例を書かないこと★** モデルは例示に極端に強く引っ張られる。
話題の選定はコード側でやり、プロンプトには枠の説明だけを書く。

### 振り返りの問いかけ

身体症状や精神的な悩みの課題は客観指標が無く、**本人の申告でしか閉じられない**。
放置すると永久に open のまま在庫になる（2026-09-11 時点で14件のうち11件は
稼働中の仮説が0件。最severe な #18(S1) すらゼロ）。

日次FBが1件だけ問いかけを出す。**「解決しましたか」とは聞かせない**（答えようがない）。
関連指標がどう変わったかを先に示し、本人が記憶や気分ではなく事実を足場に
自分を客観視できる形にする。問いかけは1回のFBに1件まで、同じ課題を続けて聞かない。

**自動では閉じない。** 誤クローズはその課題への提案が静かに止まる害を生む。
`abandoned` の誤判定で一度踏んでいる（介入0件の未検証の仮説が「反証済み」として死んだ）。

### 知識に基づく指摘

本人のデータだけでは新しく言えることが無い日は、`metric_deviations` を起点に
科学・医療・健康の一般知見で意味づけした指摘を出してよい。
制約: 一般知見であることを明示する / **精密な数値を捏造しない**（方向と機序を述べる）/
診断をしない。

**出典を引ける精度は保証できない**（モデルの内部知識に依存）。
確実な数値を出典付きで言わせたいなら RAG が必要。未着手。

## 🔄 Fitbit Web API の停止と Google Health への移行（2026-09）

**レガシー Fitbit Web API は 2026年9月に停止する**（月内の正確な日付は Google 未公表）。
Google が Fitbit を Google Health に統合し、健康データが GCP の Restricted スコープに
再分類されたため、旧APIは廃止される。

**体重・体脂肪は本体の停止より先に死んだ。** 2026-08-31 の実測を最後に
`/1/user/-/body/date/` が `{"bmi":0,"fat":0,"weight":0}` を返すだけになった。
アプリには値があるのにAPIに来ない状態で、`mrt_fitness_daily_summary.weight_kg` が
9日間 null になっていた。**体重の欠測を体重計の故障や測定習慣の問題として扱わないこと。**

理由はデータソースを見れば分かる。体重は `platform: HEALTH_CONNECT` / `GOOGLE_WEB_API` 由来で、
**Fitbit の機器を経由していない**。歩数・睡眠は `platform: FITBIT`（Pixel Watch 3）なので
旧APIを通っており、これらだけ生き残っていた。

### 移行状況

| 項目 | 取得元 |
|---|---|
| 体重・体脂肪・BMI | **Google Health**（移行済み） |
| 歩数・睡眠・心拍・カロリー | レガシー Fitbit（旧APIが死ぬまで並行稼働） |

`life_bronze_google_health`（15分ごと・直近2日）と
`life_bronze_google_health_backfill`（毎日 04:10・直近14日）の2枠で取る。
**窓を2つに分けているのは意図的。** `fitbit_flow` は対象日が「昨日と今日」だけで、
後から入った値やその時だけAPIが失敗した日が永久に欠ける。実例が 2026-08-23 の体重で、
今APIを叩けば 92.9kg が返るのに bronze は null のまま。silver の14日自己修復は
bronze が更新されない限り効かない。

### Google Health API の仕様（実測で確認したもの）

- エンドポイント `https://health.googleapis.com/v4/users/me/dataTypes/{type}/dataPoints`
- スコープは3つ。いずれも Restricted:
  `googlehealth.health_metrics_and_measurements.readonly` /
  `...activity_and_fitness.readonly` / `...sleep.readonly`
- **filter に使える項目がデータ型ごとに違う。** 単発計測は `sample_time.physical_time`、
  歩数は `interval.start_time`、**睡眠は `interval.end_time` のみ**
  （`sleep.interval.start_time` は `INVALID_DATA_POINT_FILTER_DATA_TYPE_MEMBER` で拒否）
- **ページングが必須。** 30日分の歩数は1ページ目だけなら42件、全ページ辿ると13,559件
- レスポンスのキーは lowerCamelCase で、パスの kebab-case とは別物（`body-fat` → `bodyFat`）
- Trino の JSON パスはハイフンを含むキーに `$."body-fat"` を使えない。`$["body-fat"]` と書く
- 単位は `weightGrams`（グラム）。**lbs 換算を掛けてはいけない**（旧実装の `* 0.453592` は削除済み）
- **BMI は API に無い。** 身長から自前計算する。身長は `dbt_project.yml` の
  `vars.height_m`（個人情報なのでモデルに直書きしない）

### ★歩数を HEALTH_CONNECT から取ってはいけない★

同じ実測が最大3系統（`GOOGLE_WEB_API` / `HEALTH_CONNECT` / `FITBIT_WEB_API`）から入る。
体重は `(時刻, 値)` で重複除去できるが、**歩数は区間データなので値での除去が効かない**。
デバイス単位で1つ選ぶ必要がある。実測比較（`google_health_exporter/compare_step_sources.py`）:

| 出どころ | gold比 | 相対差 | 区間の重なり |
|---|---|---|---|
| `FITBIT` / Pixel Watch 3 | 0.99倍 | 0.5% | 0 / 9 日 |
| `FITBIT` / MobileTrack | 0.75倍 | 25.2% | 0 / 9 日 |
| `HEALTH_CONNECT` | 1.83倍 | 82.8% | **9 / 9 日** |

`HEALTH_CONNECT` は**デバイス名を返さない**ためスマホの歩数計と時計を分離できず、
全日で区間が重なっていて約2倍になる。`FITBIT` 側はデバイス名が返るので
**`MobileTrack` を除外**すれば時計だけに絞れる。

**機種名で絞らないこと。** 時計を買い替えても追従させるため、除外側（`MobileTrack` は
スマホ歩数計の固定名）で判定する。旧新2台が併存する期間は日ごとに歩数最大の1台を採る。

**bronze には全ソース・全デバイスをそのまま保存している。** 選別は silver の判断。
ここで捨てると方針を変えるたびに再取り込みが必要になる
（例: 時計の充電中に空く穴をスマホの歩数で埋めたくなった場合）。

### 体重の代表値は日ごとの中央値

1日に最大10回測っていて日内で1kg以上ばらつくため、`(時刻, 値)` で重複除去した上で
中央値を採る。食事・水分のノイズに強く外れ値に引っ張られない。
**レガシーの `body/date` は日によって最初と最後がバラついていた**ので、
移行日をまたぐと数百グラムずれる。過去は遡って再計算していない
（ただし silver は直近14日を読み直すので、その窓に入る日は中央値方式で再計算される）。

### OAuth の落とし穴

- **公開ステータスが「テスト中」だと refresh token が7日で失効する。**
  認可自体も7日で切れるので、毎週ブラウザで同意を踏み直す必要がある。
  「本番」に公開すれば解消する
- **審査は不要だった。** 未認証のまま本番公開でき、代償は「確認されていないアプリ」警告と
  100ユーザーの生涯上限だけ。CASA 監査（$500〜$4,500）は上限を外すときの話
- ただし公開には**ホームページURLとプライバシーポリシーURLが必須**。
  `github.io` は「最上位のプライベートドメイン」として拒否されるため、
  `tig0826.github.io` を Search Console で所有確認して使っている
  （ページの実体は `tig0826/tig0826.github.io` リポジトリ）

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

### 移動は「滞在」と混ぜない・Google のモードを信じない
`int_timeline_outing` は `cat_main` を2つに分ける。混ぜると
**実家に数日帰省した期間が丸ごと「移動」に見える**。

| cat_main | 中身 | priority | 帯グラフでの扱い |
|---|---|---|---|
| `TRANSIT` | 実際に移動していた時間 | 85 | 帯を奪う。移動中に音楽を聴いても「移動」と出す |
| `OUTING` | 自宅以外での滞在 | 20 | あらゆる活動に譲る。滞在中は何をしていたかが見えないと思い出せない |

`activity_type` の値は `WALKING` / `IN_TRAIN` / `IN_BUS` / `IN_PASSENGER_VEHICLE` /
`IN_SUBWAY` / `IN_TRAM` / `IN_FERRY`。**`IN_VEHICLE` と `ON_BICYCLE` は存在しない**
（旧実装がこの2つで絞っていたため移動が0件になっていた）。

移動は **Google Maps のタイムラインが出しているものをそのまま使う**。
距離や速度で絞ってはいけない。一度 `distance_meters > 300` で絞ったが、それは
**バス停までの徒歩を消していた**:

```
08-20 17:07-17:12 WALKING 234m   ← バス停まで（消えていた）
08-20 17:12-17:39 IN_BUS  4230m
08-20 17:39-17:47 WALKING 264m   ← バス停から（同上）
```

実際に壊れているのは 240件中4件だけで、いずれも所要が異常に長い
（8,546分/105m、3,846分/9m、868分、433分）。次に長いのは正当な
`IN_TRAIN` 299分なので **360分で切れば誤って落とすものが無い**。
この条件だと Fitbit 歩数との相関は **r=0.836**（2026-08, n=28、絞った版は 0.814）。
自宅座標での判定は採らない（引っ越し・外泊で壊れる）。

GPS が粗く徒歩→バス→徒歩が3セグメントに割れるため、
**15分未満のギャップは同一移動として連結**する（同日内ギャップの中央値は12分）。
上の例は「17:07-17:47 40分 WALKING+IN_BUS+WALKING」と1回の移動にまとまる。

**`cat_main='OUTING'` は帯の勝者判定から除外している**（`band_candidates`）。
13時間の外出のような長い滞在があると、活動ログが無いスロットだけで OUTING が勝ち、
帯が緑と他の色でまばらに切り替わって**細切れになって何も読み取れなくなる**。
代わりに `mrt_behavior_slots_15m.is_outing` として勝者ラベルとは独立に出し、
UI が帯の直下に細い線で連続して描く。滞在中に何をしていたかは帯が、
外出していた事実は線が持つ。

`TRANSIT` は逆に**普通に帯を奪う**（priority 85）ので線には描かない（二重表示になる）。
`is_transit` / `transit_kind` はマートには残してあるので必要になれば描ける。

### ダッシュボードのイメージは必ず linux/amd64 でビルドする
Mac (arm64) で `docker build` すると arm64 イメージが `latest` に上書きされ、
ノード（amd64）で `exec format error` になり **CrashLoopBackOff で本番が落ちる**。
`latest` タグを潰しているのでロールバックも効かない。必ず:

```bash
docker buildx build --platform linux/amd64 -t tig0826/life-dashboard-ui:latest --push .
```

Prefect 側は `prefect.yaml` に `platform: linux/amd64` があるので `prefect deploy` は安全。
UI だけが手動ビルドで、ここだけガードが無い。

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

### mrt_behavior_slots_15m に --full-refresh を打つ前に確認する
`bounds` / `time_spine` の窓が全期間を覆っているか。以前は `bounds` が
`is_incremental()` の外で14日固定だったため、**full-refresh すると
直近14日だけ作り直して数ヶ月分の履歴を捨てていた**（ダッシュボードの過去日が
全部空になる）。修正済みだが、この構造のモデルは他にもあり得る。

またスパインを1本の `sequence()` で作らない。Trino の上限は1万件で、
15分刻みは1日96件なので **104日で頭打ち**になり full-refresh が
`INVALID_FUNCTION_ARGUMENT` で落ちる。日付 × 日内96オフセットの2段にする。
