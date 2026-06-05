"""
ADB-based Google Maps Timeline export automation.

Navigates Android Settings UI to trigger export without Tasker or any app.
Device-agnostic: uses text matching, not hardcoded coordinates.
"""
import subprocess
import time
import xml.etree.ElementTree as ET
import re
import os
import sys
import logging
from pathlib import Path

log = logging.getLogger(__name__)

DOWNLOADS = "/storage/emulated/0/Download"
TIMELINE_FILENAME = "タイムライン.json"
TIMELINE_REMOTE = f"{DOWNLOADS}/{TIMELINE_FILENAME}"


def adb(*args, check=False):
    cmd = ["adb", "-s", os.environ["ADB_DEVICE"]] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"adb {' '.join(args)} failed: {result.stderr}")
    return result


def dump_ui() -> ET.Element:
    adb("shell", "uiautomator", "dump", "/sdcard/_ui.xml")
    raw = adb("shell", "cat", "/sdcard/_ui.xml").stdout
    return ET.fromstring(raw)


def find_node(root: ET.Element, text: str) -> ET.Element | None:
    for node in root.iter("node"):
        if node.get("text") == text or node.get("content-desc") == text:
            return node
    return None


def tap(node: ET.Element, wait: float = 3.0):
    nums = list(map(int, re.findall(r"\d+", node.get("bounds", ""))))
    x, y = (nums[0] + nums[2]) // 2, (nums[1] + nums[3]) // 2
    log.info(f"tap: '{node.get('text') or node.get('content-desc')}' @ ({x},{y})")
    adb("shell", "input", "tap", str(x), str(y))
    time.sleep(wait)


def wait_for_node(text: str, timeout: int = 15, interval: float = 2.0) -> ET.Element:
    for _ in range(int(timeout / interval)):
        root = dump_ui()
        node = find_node(root, text)
        if node is not None:
            return node
        time.sleep(interval)
    raise TimeoutError(f"Node '{text}' not found within {timeout}s")


def find_node_partial(root: ET.Element, text: str) -> ET.Element | None:
    for node in root.iter("node"):
        t = node.get("text") or ""
        d = node.get("content-desc") or ""
        if text in t or text in d:
            return node
    return None


def _find_overflow_near(root: ET.Element, target_text: str) -> ET.Element | None:
    """target_text と同じ行にある ⋮ ボタンを返す。無名Buttonにも対応。"""
    target = find_node(root, target_text)
    if target is None:
        target = find_node_partial(root, target_text)
    if target is None:
        return None
    nums = list(map(int, re.findall(r"\d+", target.get("bounds", ""))))
    if len(nums) < 4:
        return None
    target_y = (nums[1] + nums[3]) // 2

    candidates = []
    for node in root.iter("node"):
        desc = node.get("content-desc") or ""
        text = node.get("text") or ""
        clickable = node.get("clickable") == "true"
        n = list(map(int, re.findall(r"\d+", node.get("bounds", ""))))
        if len(n) < 4:
            continue
        node_y = (n[1] + n[3]) // 2
        if abs(node_y - target_y) >= 60:
            continue
        if "その他" in desc or "オプション" in desc or text == "⋮":
            candidates.append(node)
        elif clickable and not text and not desc:
            candidates.append(node)

    if not candidates:
        return None
    # 最も右にあるもの（⋮ は行末）
    def x_center(node):
        n = list(map(int, re.findall(r"\d+", node.get("bounds", ""))))
        return (n[0] + n[2]) // 2 if len(n) >= 4 else 0
    return max(candidates, key=x_center)


def import_from_backup(source_device: str = "Pixel 7a"):
    """
    Pixel 7a のバックアップをサブ端末にインポートする。
    パス: 位置情報設定 → 位置情報サービス → タイムライン
         → このアカウントに関連付けられているデバイス → source_device ⋮ → インポート
    """
    log.info(f"=== Starting Timeline import from {source_device} ===")

    # 画面を起こす
    adb("shell", "input", "keyevent", "KEYCODE_WAKEUP")
    time.sleep(1)

    # すでにバックアップ画面にいない場合はMapsから辿る
    root = dump_ui()
    if find_node(root, source_device) is None:
        adb("shell", "am", "force-stop", "com.google.android.apps.maps")
        time.sleep(2)
        adb("shell", "am", "start", "-n",
            "com.google.android.apps.maps/com.google.android.maps.MapsActivity")
        time.sleep(6)

        # プロフィールアイコン（content-descは複数パターン対応）
        profile_labels = [
            "アカウントと設定。",
            "プロフィール画像を変更します。",
            "アカウントとヘルプ",
            "Account and help",
        ]
        profile_node = None
        for _ in range(10):
            root = dump_ui()
            for label in profile_labels:
                profile_node = find_node(root, label)
                if profile_node is not None:
                    break
            if profile_node is not None:
                break
            time.sleep(2)
        if profile_node is None:
            raise RuntimeError(
                "Profile icon not found. nodes: " +
                str([n.get("content-desc") for n in root.iter("node") if n.get("content-desc")])
            )
        tap(profile_node)
        tap(wait_for_node("タイムライン", timeout=10))

        # タイムライン VIEW が完全に描画されるまで待つ（雲アイコンが現れるまでリトライ）
        # 雲マーク（バックアップ無効）= desc "バックアップは無効です。"
        # バックアップ有効時は別のdesc になる可能性があるので "バックアップ" 部分一致で探す
        backup_icon = None
        for _ in range(15):
            time.sleep(2)
            root = dump_ui()
            for n in root.iter("node"):
                d = n.get("content-desc") or ""
                if "バックアップ" in d:
                    backup_icon = n
                    break
            if backup_icon is not None:
                break

        if backup_icon is not None:
            log.info("Tapping cloud/backup icon: desc=%s", backup_icon.get("content-desc"))
            tap(backup_icon)
        else:
            # フォールバック: タイムラインViewの右上バックアップアイコンを座標でタップ
            log.info("Backup icon not found via desc, tapping by coordinate (top-right)")
            adb("shell", "input", "tap", "710", "72")
            time.sleep(3)
        wait_for_node("暗号化バックアップのスイッチ", timeout=20)

    # source_device が現れるまで待つ
    for _ in range(10):
        time.sleep(2)
        if find_node(dump_ui(), source_device) is not None:
            break

    # source_device 行の ⋮ を探してタップ
    root = dump_ui()
    log.info("Backup screen nodes: %s", [
        n.get("text") or n.get("content-desc")
        for n in root.iter("node")
        if n.get("text") or n.get("content-desc")
    ])
    overflow = _find_overflow_near(root, source_device)
    if overflow is None:
        raise RuntimeError(
            f"⋮ button not found near '{source_device}'. "
            "nodes: " + str([n.get("text") or n.get("content-desc") for n in root.iter("node")])
        )
    tap(overflow, wait=2.0)

    # ⋮ メニューの中身をログ出力してからタップ
    root = dump_ui()
    log.info("Overflow menu nodes: %s", [
        n.get("text") or n.get("content-desc")
        for n in root.iter("node")
        if n.get("text") or n.get("content-desc")
    ])

    for label in ["インポート", "このデバイスにインポート", "復元"]:
        try:
            tap(wait_for_node(label, timeout=5))
            break
        except TimeoutError:
            continue
    else:
        raise RuntimeError("Import menu option not found")

    # 確認ダイアログが出た場合
    time.sleep(2)
    root = dump_ui()
    for label in ["インポート", "続行", "OK"]:
        node = find_node(root, label)
        if node is not None:
            tap(node)
            break

    # インポート完了待ち
    log.info("Waiting for import to complete...")
    for _ in range(24):  # 最大2分
        time.sleep(5)
        root = dump_ui()
        if find_node_partial(root, "インポート中") is None and find_node_partial(root, "読み込み中") is None:
            break

    adb("shell", "input", "keyevent", "KEYCODE_HOME")
    log.info(f"Import from {source_device} complete")


def delete_old_exports():
    """エクスポート前に旧ファイルを削除してファイル名を固定する。"""
    result = adb("shell", f"ls '{DOWNLOADS}/'")
    for line in result.stdout.splitlines():
        if "タイムライン" in line and ".json" in line:
            path = f"{DOWNLOADS}/{line.strip()}"
            adb("shell", "rm", "-f", path)
            log.info(f"Deleted: {path}")


def export_timeline():
    log.info("=== Starting Timeline export ===")

    delete_old_exports()

    # Step 1: Location Settings
    adb("shell", "am", "start", "-a", "android.settings.LOCATION_SOURCE_SETTINGS")
    time.sleep(3)

    # Step 2: 位置情報サービス
    tap(wait_for_node("位置情報サービス"))

    # Step 3: タイムライン
    tap(wait_for_node("タイムライン"))

    # Step 4: タイムラインをエクスポート
    tap(wait_for_node("タイムラインをエクスポート"))

    # Step 5: 続行（確認ダイアログ）
    tap(wait_for_node("続行"))

    # Step 6: 生体認証チェック（サブ端末ではスキップされるはず）
    time.sleep(3)
    root = dump_ui()
    if find_node(root, "指紋認証センサーをタッチ") is not None or find_node(root, "本人確認") is not None:
        raise RuntimeError(
            "Biometric auth required. Sub-device must have no screen lock."
        )

    # Step 7: 保存（ファイルピッカー）
    tap(wait_for_node("保存", timeout=20))

    # エクスポート完了まで待機（ファイルサイズ安定を確認）
    log.info("Waiting for export to complete...")
    prev_size = -1
    for _ in range(30):
        time.sleep(5)
        result = adb("shell", f"stat -c %s '{TIMELINE_REMOTE}' 2>/dev/null || echo 0")
        size = int(result.stdout.strip())
        log.info(f"  file size: {size} bytes")
        if size > 0 and size == prev_size:
            break
        prev_size = size

    # 戻るボタンで設定アプリを閉じる
    adb("shell", "input", "keyevent", "KEYCODE_HOME")

    log.info(f"Export complete: {TIMELINE_REMOTE} ({prev_size} bytes)")
    return TIMELINE_REMOTE


def pull_timeline(local_path: str) -> str:
    log.info(f"Pulling {TIMELINE_REMOTE} -> {local_path}")
    adb("pull", TIMELINE_REMOTE, local_path, check=True)
    return local_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    export_timeline()
    pull_timeline("/tmp/timeline.json")
