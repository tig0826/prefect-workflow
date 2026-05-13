# Google Maps Timeline Exporter

## 概要

Android設定アプリの「タイムラインをエクスポート」をADB経由で自動化し、
差分データをMinIO Bronzeに取り込むパイプライン。

## なぜ物理Androidデバイスが必要か

Google Play Services（GMS）はライセンス制で物理デバイスのみ動作。
タイムラインデータはGMSがデバイスローカルに保存するため、
VM/コンテナ/エミュレータでは取得不可。

---

## デバイス要件（サブ端末）

- Android 11以降（WiFi ADB標準搭載）
- **画面ロックなし・生体認証なし**（エクスポート時の本人確認をスキップするため）
- Googleアカウントにサインイン済み（タイムラインON）
- Tailscaleインストール済み・常時接続
- 電源常時供給（自宅固定運用）

---

## 自動化フローの詳細（ADB UIAutomator方式）

調査日: 2026-05-10（Pixel 9, Android 16, GMS 261631000で確認）

```
am start -a android.settings.LOCATION_SOURCE_SETTINGS
  ↓ tap: 「位置情報サービス」
  ↓ tap: 「タイムライン」
  ↓ tap: 「タイムラインをエクスポート」
  ↓ tap: 「続行」（確認ダイアログ）
  ↓ [本人確認] ← 画面ロックなしの端末ではスキップされる想定
  ↓ tap: 「保存」（ファイルピッカー / Downloads固定）
  ↓ 保存先: /storage/emulated/0/Download/タイムライン.json
```

重要: `OnDeviceSettingsV31Activity` (com.google.android.gms) は not-exported のため
`am start -n` で直接起動不可。必ず設定アプリ経由でナビゲートする。

エクスポートされるファイル:
- ファイル名: `タイムライン.json`（既存の場合は `タイムライン (N).json`）
- サイズ: 全期間で約85MB（毎回フルエクスポート、増加し続ける）
- フォーマット: `{ "semanticSegments": [...] }`

セグメント種別:
- `visit`: 滞在場所（placeId、確率付き）
- `activity`: 移動（WALKING / IN_VEHICLE / ON_BICYCLE / RUNNING）
- `timelinePath`: 生GPSトラック（補完用、Silverまで）

---

## アーキテクチャ

```
Prefect (6h cron, Kubernetes)
  → ADB over Tailscale → Androidサブ端末
  → export_timeline.py でエクスポート発火
  → adb pull タイムライン.json
  → sync_timeline.py で差分抽出（watermark方式）
  → MinIO s3a://bronze-zone/timeline/dt=YYYY-MM-DD/
  → Hive external table: hive.life_bronze.timeline_external
  → dbt: silver/timeline_segments.sql
  → dbt: intermediate/int_timeline_outing.sql
  → dbt: int_all_behavior_events (UNION ALL追加)
```

差分方式:
- サーバー側でmax(end_time)をwatermarkとして管理
- フルファイル(85MB)はADB pull（LAN内転送、数秒）
- watermark以降のsegmentのみMinIOに書き込む（数KB〜数十KB/日）

---

## ファイル構成

```
timeline_exporter/
  setup.sh              # サブ端末到着時に1回実行
  export_timeline.py    # ADB自動化スクリプト（エクスポート発火）
  sync_timeline.py      # 差分抽出・MinIOアップロード
  timeline_flow.py      # Prefectフロー本体
```

---

## WiFi / 接続について

- サブ端末は自宅固定 → 常にTailscale経由でアクセス可能
- WiFi確認不要（物理的に自宅のみ運用）
- デバイスIPはTailscale固定IP: 設定後 `setup.sh` が記録する
- ADB接続コマンド: `adb connect <tailscale_ip>:<port>`

---

## Shizukuについて

デバイスにShizukuがインストール済みの場合、`rish`でshellレベルの権限が得られるが、
`/data/data/com.google.android.gms/` はroot権限が必要でアクセス不可。
GMSのローカルDBへの直接アクセスはroot端末のみ対応可能。
