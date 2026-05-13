#!/usr/bin/env bash
# =============================================================================
# Google Maps Timeline Exporter - Sub-device Setup
# =============================================================================
# 実行方法: bash setup.sh
# 前提:
#   - Mac/Linux で実行
#   - adb が PATH にあること (brew install android-platform-tools)
#   - サブ端末のAndroid設定 > 開発者向けオプション > ワイヤレスデバッグ が ON
#   - サブ端末のTailscale IPが判明していること
# =============================================================================

set -euo pipefail

ADB=${ADB:-"$HOME/Library/Android/sdk/platform-tools/adb"}
if ! command -v adb &>/dev/null; then
  if [ -f "$ADB" ]; then
    export PATH="$(dirname $ADB):$PATH"
  else
    echo "ERROR: adb not found. Install via: brew install android-platform-tools"
    exit 1
  fi
fi

echo "=================================================="
echo " Step 1: サブ端末との接続"
echo "=================================================="
echo ""
echo "サブ端末のTailscale IPとADBポートを入力してください。"
echo "（設定 > 開発者向けオプション > ワイヤレスデバッグ > IPアドレスとポート）"
read -p "ADB_DEVICE (例: 100.100.x.x:12345): " ADB_DEVICE

echo ""
echo "ペアリングが必要な場合は以下を入力してください。（不要な場合は空Enter）"
read -p "Pairing IP:PORT (例: 100.100.x.x:34313): " PAIR_ADDR
read -p "Pairing CODE (例: 485575): " PAIR_CODE

if [ -n "$PAIR_ADDR" ] && [ -n "$PAIR_CODE" ]; then
  echo "Pairing..."
  adb pair "$PAIR_ADDR" "$PAIR_CODE"
fi

echo "Connecting to $ADB_DEVICE ..."
adb connect "$ADB_DEVICE"
adb -s "$ADB_DEVICE" devices

echo ""
echo "=================================================="
echo " Step 2: デバイス要件チェック"
echo "=================================================="

echo -n "Android version: "
adb -s "$ADB_DEVICE" shell getprop ro.build.version.release

echo -n "Screen lock type: "
LOCK=$(adb -s "$ADB_DEVICE" shell settings get secure lockscreen.password_type 2>/dev/null || echo "unknown")
echo "$LOCK"
if [ "$LOCK" != "0" ] && [ "$LOCK" != "" ] && [ "$LOCK" != "unknown" ]; then
  echo "WARNING: 画面ロックが設定されています (type=$LOCK)"
  echo "  設定 > セキュリティ > 画面ロック > なし に変更してください"
fi

echo ""
echo "=================================================="
echo " Step 3: 動作テスト（エクスポート1回実行）"
echo "=================================================="
read -p "テストエクスポートを実行しますか？ [y/N]: " RUN_TEST

if [ "$RUN_TEST" = "y" ] || [ "$RUN_TEST" = "Y" ]; then
  ADB_DEVICE="$ADB_DEVICE" python3 "$(dirname "$0")/export_timeline.py"
  echo ""
  echo -n "Export file check: "
  adb -s "$ADB_DEVICE" shell ls -lh /storage/emulated/0/Download/ | grep "タイムライン"
fi

echo ""
echo "=================================================="
echo " Step 4: Prefect Variable 登録"
echo "=================================================="
echo "以下のコマンドをPrefect環境で実行してデバイスIPを登録してください："
echo ""
echo "  prefect variable set timeline-adb-device \"$ADB_DEVICE\""
echo ""
read -p "今すぐ実行しますか？ [y/N]: " RUN_PREFECT

if [ "$RUN_PREFECT" = "y" ] || [ "$RUN_PREFECT" = "Y" ]; then
  cd "$(dirname "$0")/.."
  uv run prefect variable set timeline-adb-device "$ADB_DEVICE"
  echo "Prefect variable 'timeline-adb-device' registered."
fi

echo ""
echo "=================================================="
echo " Step 5: Dockerfile に adb を追加"
echo "=================================================="
echo "Dockerfileに以下を追加が必要です（life_dashboard/Dockerfile）："
echo ""
echo "  RUN apt-get update && apt-get install -y --no-install-recommends android-tools-adb \\"
echo "    && rm -rf /var/lib/apt/lists/*"
echo ""
echo "  COPY --chown=prefectuser:prefectgroup ./timeline_exporter/ /opt/prefect/timeline_exporter"
echo ""
read -p "今すぐDockerfileを更新しますか？ [y/N]: " UPDATE_DOCKERFILE

if [ "$UPDATE_DOCKERFILE" = "y" ] || [ "$UPDATE_DOCKERFILE" = "Y" ]; then
  DOCKERFILE="$(dirname "$0")/../Dockerfile"
  # adbインストールをRUNブロックに追加
  sed -i.bak 's/RUN apt-get update && apt-get install -y --no-install-recommends \\/RUN apt-get update \&\& apt-get install -y --no-install-recommends \\\n    android-tools-adb \\/' "$DOCKERFILE"
  # COPYブロック追加
  sed -i.bak '/COPY --chown=prefectuser:prefectgroup .\/ai_feedback\//a COPY --chown=prefectuser:prefectgroup .\/timeline_exporter\/ \/opt\/prefect\/timeline_exporter' "$DOCKERFILE"
  echo "Dockerfile updated. 差分確認: git diff life_dashboard/Dockerfile"
fi

echo ""
echo "=================================================="
echo " Step 6: prefect.yaml にデプロイ設定を追加"
echo "=================================================="
echo "prefect.yaml の deployments セクションに以下を追加してください："
echo ""
cat << 'YAML'
  - name: life_timeline_sync
    entrypoint: timeline_exporter/timeline_flow.py:timeline_sync_flow
    work_pool:
      name: kubernetes-work-pool
    schedule:
      cron: "0 */6 * * *"
      timezone: Asia/Tokyo
    build:
      - prefect_docker.deployments.steps.build_docker_image:
          id: build-image
          requires: prefect-docker>=0.6.0
          image_name: tig0826/life-prefect
          tag: latest
          dockerfile: Dockerfile
    push:
      - prefect_docker.deployments.steps.push_docker_image:
          requires: prefect-docker>=0.6.0
          image_name: "{{ build-image.image_name }}"
          tag: "{{ build-image.tag }}"
    pull:
      - prefect.deployments.steps.set_working_directory:
          directory: /opt/prefect
YAML
echo ""
echo "=================================================="
echo " 完了"
echo "=================================================="
echo ""
echo "次のステップ:"
echo "  1. Dockerfile更新 & prefect.yaml更新を確認"
echo "  2. git commit && git push"
echo "  3. uv run prefect deploy --all"
echo "  4. 初回の全量インポートを手動実行:"
echo "     ADB_DEVICE=$ADB_DEVICE python3 timeline_exporter/export_timeline.py"
echo "     python3 timeline_exporter/sync_timeline.py /tmp/timeline.json"
echo ""
echo "ADB_DEVICE=$ADB_DEVICE を .env や環境設定に保存してください。"
