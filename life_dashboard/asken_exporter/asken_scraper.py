import argparse
import json
import logging
import os
import re
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, Tag

# ロガーの設定: スクリプトの動作状況を記録する
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class AskenScraper:
    """
    あすけん (asken.jp) から食事と栄養の記録をスクレイピングするクラス。

    使用方法:
        scraper = AskenScraper("email@example.com", "password")
        scraper.login()
        data = scraper.fetch_daily_data(date(2026, 2, 15))
    """

    BASE_URL = "https://www.asken.jp"
    LOGIN_URL = f"{BASE_URL}/login"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    def __init__(self, email: str, password: str):
        """
        Scraperを初期化する。

        Args:
            email: あすけんのログインメールアドレス。
            password: ログインパスワード。
        """
        if not email or not password:
            raise ValueError("メールアドレスとパスワードは必須です。")
        self.email = email
        self.password = password
        self.session = requests.Session()
        retries = Retry(
            total=3,  # 最大3回リトライ
            backoff_factor=1,  # 1秒, 2秒, 4秒と待機時間を増やす
            status_forcelist=[500, 502, 503, 504],  # サーバーエラー時のみリトライ
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"User-Agent": self.USER_AGENT})
        self._is_logged_in = False

    def _build_login_payload(self) -> Dict[str, str]:
        """ログインページを解析し、POSTリクエスト用のペイロードを構築する。"""
        try:
            logging.info("ログインフォームのデータを取得中...")
            response = self.session.get(self.LOGIN_URL)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            payload = {}
            # ログインフォームのformタグ内を探索
            login_form = soup.find("form")
            if not isinstance(login_form, Tag):
                raise IOError("ログインフォームが見つかりません。")

            for tag in login_form.find_all("input"):
                name = tag.get("name")
                value = tag.get("value", "")
                if name:
                    payload[name] = value

            # ユーザーの認証情報でペイロードを上書き
            payload["data[CustomerMember][email]"] = self.email
            payload["data[CustomerMember][passwd_plain]"] = self.password

            logging.info("ログインペイロードの構築に成功。")
            return payload
        except requests.RequestException as e:
            logging.error(f"ログインフォーム取得中にネットワークエラーが発生しました: {e}")
            raise

    def login(self):
        """あすけんにログインする。"""
        if self._is_logged_in:
            return

        payload = self._build_login_payload()

        try:
            logging.info("認証ペイロードを送信中...")
            self.session.headers.update({"Referer": self.LOGIN_URL})
            response = self.session.post(self.LOGIN_URL, data=payload)
            response.raise_for_status()
            time.sleep(2)  # サーバーへの負荷軽減

            # ログイン成功を判定 (レスポンス内容にログアウトリンクがあるか)
            soup = BeautifulSoup(response.text, "html.parser")
            if not soup.select_one('a[href*="logout"]'):
                # === デバッグ機能 ===
                # debug_filename = f"asken_login_failed_{int(time.time())}.html"
                # with open(debug_filename, "w", encoding="utf-8") as f:
                #     f.write(response.text)
                # ===================
                raise PermissionError(
                    "ログインに失敗しました。認証情報が間違っているか、サイトの仕様が変更された可能性があります。"
                )

            self._is_logged_in = True
            logging.info("ログインに成功しました。")
        except requests.RequestException as e:
            logging.error(f"ログイン中にネットワークエラーが発生しました: {e}")
            raise

    @staticmethod
    def _parse_value_and_unit(text: str) -> Tuple[Optional[float], Optional[str]]:
        """ '123.4g' のような文字列を数値と単位に分割する。"""
        text = text.strip()
        # 正規表現で数値部分と単位部分を抽出
        match = re.match(r"^(-?\d+(?:\.\d+)?)(.*)$", text)
        if match:
            value_str, unit = match.groups()
            try:
                return float(value_str), unit.strip()
            except ValueError:
                return None, text
        return None, text

    def _parse_meal_records(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """食事記録のセクションを解析する。"""
        logging.info("食事記録を解析中...")
        records = {}
        categories = ["breakfast", "lunch", "dinner", "sweets", "exercise"]
        for category in categories:
            section = soup.select_one(f"#karute_report_{category}")
            if not isinstance(section, Tag):
                continue

            total_energy_tag = section.select_one(".sum_energy")
            total_kcal, _ = self._parse_value_and_unit(
                total_energy_tag.text if total_energy_tag else "0kcal"
            )

            items = []
            rows = section.select("table.detail_table tr")
            for row in rows:
                name_tag = row.select_one("td.name")
                qty_tag = row.select_one("td.quantity")
                energy_tag = row.select_one("td.energy")

                if name_tag and energy_tag:
                    kcal, _ = self._parse_value_and_unit(energy_tag.text)
                    items.append({
                        "name": name_tag.text.strip(),
                        "quantity": qty_tag.text.strip() if qty_tag else None,
                        "kcal": kcal,
                    })
            records[category] = {"total_kcal": total_kcal, "items": items}
        return records

    def _parse_nutrition_summary(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """栄養素のサマリーを解析する。"""
        logging.info("栄養サマリーを解析中...")
        nutrition_data = {}
        list_items = soup.select("li.line_left ul.left")
        for item in list_items:
            title_tag = item.select_one("li.title")
            val_tag = item.select_one("li.val")

            if title_tag and val_tag:
                key = title_tag.text.strip()
                value, unit = self._parse_value_and_unit(val_tag.text)
                nutrition_data[key] = {"value": value, "unit": unit}
        return nutrition_data

    def fetch_daily_data(self, target_date: date) -> Dict[str, Any]:
        """
        指定された日付の食事と栄養のデータを取得する。

        Args:
            target_date: 取得対象の日付。

        Returns:
            その日の記録と栄養サマリーを含むdict。
        """
        # --- 堅牢性向上のための型変換 ---
        if isinstance(target_date, str):
            try:
                target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError("日付文字列の形式が不正です。YYYY-MM-DD形式で指定してください。")
        # -----------------------------------

        if not self._is_logged_in:
            self.login()

        date_str = target_date.strftime("%Y-%m-%d")
        comment_url = f"{self.BASE_URL}/wsp/comment/{date_str}"
        advice_url = f"{self.BASE_URL}/wsp/advice"

        try:
            # 最初に `comment` ページにアクセスして、セッションに日付を記録させる
            logging.info(f"{date_str} のカレンダーページにアクセス中...")
            comment_response = self.session.get(comment_url)
            comment_response.raise_for_status()
            time.sleep(1)

            # 次に `advice` ページにアクセスして、その日付のサマリーデータを取得
            logging.info("アドバイスページからサマリーを取得中...")
            advice_response = self.session.get(advice_url)
            advice_response.raise_for_status()

            # commentページのHTMLから食事記録を、adviceページのHTMLからサマリーを取得
            comment_soup = BeautifulSoup(comment_response.text, "html.parser")
            advice_soup = BeautifulSoup(advice_response.text, "html.parser")

            meal_records = self._parse_meal_records(comment_soup)
            nutrition_summary = self._parse_nutrition_summary(advice_soup)
            
            logging.info("データ抽出完了。")

            return {
                "date": date_str,
                "meal_records": meal_records,
                "nutrition_summary": nutrition_summary,
            }
        except requests.RequestException as e:
            logging.error(f"データ取得中にネットワークエラーが発生しました: {e}")
            raise
        except Exception as e:
            logging.error(f"データ解析中に予期せぬエラーが発生しました: {e}")
            raise

def main():
    """スクリプトのエントリーポイント。"""
    parser = argparse.ArgumentParser(description="あすけんから指定日の食事・栄養データを取得します。")
    parser.add_argument(
        "-d", "--date",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help="取得する日付 (YYYY-MM-DD形式)。デフォルトは今日。",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        help="結果を保存するJSONファイルのパス。指定しない場合は標準出力に表示。",
    )
    args = parser.parse_args()

    try:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logging.error("日付の形式が不正です。YYYY-MM-DD形式で指定してください。")
        return

    # 環境変数から認証情報を取得
    email = os.getenv("ASKEN_EMAIL")
    password = os.getenv("ASKEN_PASSWORD")

    if not email or not password:
        logging.error("環境変数 ASKEN_EMAIL と ASKEN_PASSWORD を設定してください。")
        return

    try:
        scraper = AskenScraper(email, password)
        daily_data = scraper.fetch_daily_data(target_date)

        # JSONシリアライズ可能な形式に変換
        output_json = json.dumps(daily_data, ensure_ascii=False, indent=2)

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output_json)
            logging.info(f"データを {args.output} に保存しました。")
        else:
            print(output_json)

    except (ValueError, IOError, PermissionError, requests.RequestException) as e:
        logging.error(f"処理が失敗しました: {e}")
    except Exception as e:
        logging.error(f"予期せぬエラーが発生しました: {e}")


if __name__ == "__main__":
    main()
