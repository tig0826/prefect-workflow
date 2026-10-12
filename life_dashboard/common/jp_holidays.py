"""日本の祝日・休日（振替休日・国民の休日を含む）。業務時間を「勤務日だけ」で平均するために使う。

出典: 内閣府「国民の祝日について」 https://www8.cao.go.jp/chosei/shukujitsu/gaiyou.html
★毎年2月に翌年の春分・秋分が官報で決まる。2028年分はそのとき足すこと★
UI 側にも同じ表がある（life_dashboard_ui/lib/jp-holidays.ts）。有給などの休暇もそちらと揃えること。
"""

import datetime

JP_HOLIDAYS = frozenset({
    # 2026（令和8年）
    "2026-01-01", "2026-01-12", "2026-02-11", "2026-02-23", "2026-03-20", "2026-04-29",
    "2026-05-03", "2026-05-04", "2026-05-05", "2026-05-06", "2026-07-20", "2026-08-11",
    "2026-09-21", "2026-09-22", "2026-09-23", "2026-10-12", "2026-11-03", "2026-11-23",
    # 2027（令和9年）
    "2027-01-01", "2027-01-11", "2027-02-11", "2027-02-23", "2027-03-21", "2027-03-22",
    "2027-04-29", "2027-05-03", "2027-05-04", "2027-05-05", "2027-07-19", "2027-08-11",
    "2027-09-20", "2027-09-23", "2027-10-11", "2027-11-03", "2027-11-23",
})

# 有給休暇・会社の休み。データからは休んだのかサボったのか区別できないので、分かっているものだけ足す
DAYS_OFF: frozenset[str] = frozenset()


def is_workday(d: datetime.date | str) -> bool:
    if isinstance(d, str):
        d = datetime.date.fromisoformat(d)
    s = d.isoformat()
    return d.weekday() < 5 and s not in JP_HOLIDAYS and s not in DAYS_OFF


def non_workday_sql_list() -> str:
    """SQL の IN 句に入れる祝日・休暇の DATE リテラル（土日は day_of_week で別に除く）"""
    return ", ".join(f"DATE '{s}'" for s in sorted(JP_HOLIDAYS | DAYS_OFF))
