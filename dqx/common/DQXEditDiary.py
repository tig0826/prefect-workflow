import re

from bs4 import BeautifulSoup

from common.session_cookies import reuse_session

class DQXEditDiary:
    def __init__(self, character_id):
        self.session = reuse_session()
        self.base_url = "https://hiroba.dqx.jp"
        self.create_url = f"{self.base_url}/sc/diary/{character_id}/create/2/0"
        self.confirm_url = f"{self.create_url}/confirm"
        self.success_url = f"{self.create_url}/success"
        self.user_agent = {"User-Agent": "Mozilla/5.0"}
        self.diary_list_url = f"{self.base_url}/sc/diary/{character_id}/mode/2/page/0"
        self.delete_base_url = f"{self.base_url}/sc/diary/{character_id}/delete/2"

    def _extract_hidden_inputs(self, soup: BeautifulSoup): 
        """confirm 画面の hidden input を name:value の辞書にして返す"""
        return {
            i.get("name"): i.get("value", "")
            for i in soup.find_all("input", type="hidden")
            if i.get("name")
        }

    def post_diary(self,
                   category_num,
                   title,
                   message):
        u"""
        冒険者の広場に日誌を投稿する。
        category_num: カテゴリ番号。ルームメンバー募集は50。
        message: 投稿する内容
        """
        # self.session.headers.update({
        #     "User-Agent": "Mozilla/5.0",
        # })
        resp0 = self.session.get(self.create_url, headers=self.user_agent)
        soup0 = BeautifulSoup(resp0.content, 'html.parser')
        hidden0 = self._extract_hidden_inputs(soup0)
        payload = {
            "diaryCategory": category_num,
            "diaryTitle": title,
            "diaryText": message,
            "publicityflag": "5",
            "commentFlag": "1",
            "type": "0",
            **hidden0,
        }
        headers1 = {
            **self.user_agent,
            "Origin": self.base_url,
            "Referer": self.create_url,
        }
        resp1 = self.session.post(self.confirm_url, data=payload, headers=headers1, allow_redirects=True)
        soup1 = BeautifulSoup(resp1.content, 'html.parser')
        hidden = self._extract_hidden_inputs(soup1)
        payload2 = {**payload, **hidden}
        headers2 = {
            **self.user_agent,
            "Origin": self.base_url,
            "Referer": self.confirm_url,
        }
        resp2 = self.session.post(self.success_url, data=payload2, headers=headers2, allow_redirects=True)

        return resp2.status_code, resp2

    def get_diary_id_list(self, title):
        resp = self.session.get(self.diary_list_url, headers=self.user_agent)
        soup = BeautifulSoup(resp.content, 'html.parser')
        diary_list = soup.find_all(class_="article")
        id_list = []
        for diary in diary_list:
            tl = diary.find(class_="title_diary")
            t = tl.text.strip()
            if t == title:
                link = tl.a['href']
                m = re.search(r"/view/(\d+)", link)
                diary_id = m.group(1)
                id_list.append(diary_id)
        return id_list

    def delete_diary(self, diary_id):
        delete_confirm_url = f"{self.delete_base_url}/{diary_id}/{diary_id}/confirm"
        delete_success_url = f"{self.delete_base_url}/{diary_id}/{diary_id}/success"
        resp1 = self.session.post(delete_confirm_url, headers=self.user_agent)
        soup1 = BeautifulSoup(resp1.content, 'html.parser')
        hidden = self._extract_hidden_inputs(soup1)
        headers2 = {
            **self.user_agent,
            "Origin": self.base_url,
            "Referer": delete_confirm_url,
        }
        resp2 = self.session.post(delete_success_url,
                                  data=hidden,
                                  headers=headers2,
                                  allow_redirects=True)

        return resp2.status_code, resp2

