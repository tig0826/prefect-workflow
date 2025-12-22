from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

from common.DQXEditDiary import DQXEditDiary


@task(retries=10, retry_delay_seconds=5, log_prints=True)
def post_discord_link_diary():
    character_id = Secret.load("dqx-character-id").get()
    dqx_edit_diary = DQXEditDiary(character_id=character_id)
    title = "培養輝晶獣discord・ルームメンバー募集"
    body = "\n".join(Variable.get("dqx-discord-link-diary-body"))

    old_diary_id_list = dqx_edit_diary.get_diary_id_list(title)
    for old_diary_id in old_diary_id_list:
        dqx_edit_diary.delete_diary(old_diary_id)
    # 日誌を投稿
    dqx_edit_diary.post_diary(category_num=50, title=title, message=body)
    # 投稿時は必ず非公開になっているので全体公開に変更する
    diary_id_list = dqx_edit_diary.get_diary_id_list(title)
    for diary_id in diary_id_list:
        dqx_edit_diary.change_diary(diary_id=diary_id,
                                    category_num=50,
                                    title=title,
                                    message=body,
                                    publicity=5)


@flow(name="post_diary", log_prints=True)
def post_diary():
    post_discord_link_diary()

