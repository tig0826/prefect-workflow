# Life Dashboard Data Pipeline

各種ソース（Fitbit, ActivityWatch, Asken, OwnTracks等）からライフログを収集し、分析・可視化用に加工・提供するためのデータパイプラインです。

## 🏗 アーキテクチャ

1.  **Exporters**: Python (Prefect) を使用し、各種APIやDBからデータを取得して S3 (Iceberg) に格納します。
2.  **Trino**: 全てのデータのクエリ・エンジンとして機能します。
3.  **dbt**: Trino 上でデータ変換を行い、Bronze (Raw) -> Silver (Cleaned) -> Gold (Mart) の順に加工します。
4.  **Gold Layer**: 可視化に最適化されたデータ（例：`mrt_behavior_slots_15m`）を `life_gold` スキーマに保持します。

## 📂 プロジェクト構成

- `asken_exporter/`: あすけん（食事・栄養）データ収集
- `aw_exporter/`: ActivityWatch（PC作業・AFK）データ収集
- `fitbit_exporter/`: Fitbit（睡眠・歩数・心拍数）データ収集
- `owntracks_exporter/`: OwnTracks（位置情報・外出）データ収集
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
加工済みのデータは、別プロジェクト `dashboard` (Next.js) から Trino 経由で参照されます。
特に `life_gold.mrt_behavior_slots_15m` テーブルが、ダッシュボードの行動タイムラインのメインデータソースとなります。
