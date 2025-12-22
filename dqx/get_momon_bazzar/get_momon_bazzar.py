from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import time
from zoneinfo import ZoneInfo

from prefect import flow, task

from common.session_cookies import reuse_session
from common.discord_notify import send_to_discord


class DQXDiarySearch:
    def __init__(self):
        # セッションを再利用
        self.session = reuse_session()

    def search_diary(self, category_num, search_word):
        u"""
        冒険者の広場の日記を検索する
        category_num: カテゴリ番号。モーモンバザーは12
        search_word: 検索ワード
        """
        base_url = "https://hiroba.dqx.jp"
        url = f"{base_url}/sc/diary/pub/search/"
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Referer": f"{base_url}/sc/diary/pub/list?diaryCategory={category_num}",
        })
        payload = {
            "diarySearchWord": search_word,
            "diaryCategory": category_num,
        }
        resp = self.session.post(url, data=payload)
        soup = BeautifulSoup(resp.content, 'html.parser')
        diary_list = soup.find_all(class_="thread_article")
        # 直近10分の記事を抽出して表示
        valid_diaries = []
        # 10分前の時刻を計算
        JST = ZoneInfo("Asia/Tokyo")
        now = datetime.now(JST)
        boundary = now.replace(second=0, microsecond=0,
                               minute=(now.minute // 10) * 10)
        window_start = boundary - timedelta(minutes=10)
        window_end   = boundary
        for diary in diary_list:
            tl = diary.find(class_="title_diary")
            title = tl.text.strip()
            link = base_url + tl.a['href']
            author = diary.find(class_="txt_name").text.strip()
            t = diary.find(class_="threadlist_catday").get_text(" ", strip=True)
            m = re.search(r"\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?", t)
            dt_str = m.group(0) if m else None
            date = datetime.fromisoformat(dt_str.replace(" ", "T")).replace(tzinfo=JST)
            content = diary.find(class_="txt_comment").text.strip()
            if window_start <= date < window_end:
                # 最新の10分以内の記事のみ表示
                valid_diary = {
                               "title": title,
                               "link": link,
                               "author": author,
                               "posting_time": date,
                               "content": content
                               }
                valid_diaries.append(valid_diary)
        return valid_diaries


@task(retries=10, retry_delay_seconds=5, log_prints=True)
def search_diary():
    dqx_diary_search = DQXDiarySearch()
    diaries = dqx_diary_search.search_diary(category_num=12, search_word="細胞")
    for diary in diaries:
        title = diary["title"]
        link = diary["link"]
        author = diary["author"]
        date = diary["posting_time"].strftime("%Y-%m-%d %H:%M")
        content = diary["content"]
        message = f"Title: {title}\nLink: {link}\nAuthor: {author}\nDate: {date}\n{content}\n{'-'*40}"
        send_to_discord(message, discord_webhook_url_name="discord-webhook-url-momon-bazzar")


@flow(name="get_momon_bazzar", log_prints=False)
def get_momon_bazzar():
    time.sleep(1.5)
    search_diary()

