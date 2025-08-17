from datetime import datetime, timedelta, timezone
from os import wait
import pytz

class DatetimeTranslator:
    def __init__(self):
        utc_now = datetime.now(timezone.utc)
        jst = pytz.timezone('asia/tokyo')
        self.jst_now = utc_now.astimezone(jst)
    def _sub_days(self, jst_now, days, weeks, months):
        jst = jst_now + timedelta(days=days) + timedelta(weeks=weeks) + timedelta(days=months*30)
        return jst
    def _replace_hour(self, jst, replace_hour):
        if replace_hour is not None:
            jst = jst.replace(hour=replace_hour, minute=0, second=0, microsecond=0)
        return jst
    def datetime(self, days=0, weeks=0, months=0, replace_hour=None):
        jst = self._sub_days(self.jst_now, days, weeks, months)
        jst = self._replace_hour(jst, replace_hour)
        datetime_info = jst.strftime("%Y-%m-%d %H:%M:%S")
        return datetime_info
    def date(self, days=0, weeks=0, months=0):
        jst = self._sub_days(self.jst_now, days, weeks, months)
        date_info = jst.strftime("%Y-%m-%d")
        return date_info
    def hour(self, replace_hour=None):
        jst = _replace_hour(jst, replace_hour)
        hour_info = jst.strftime("%H")
        return hour_info
    def weekday(self, days=0, weeks=0, months=0):
        jst = self._sub_days(self.jst_now, days, weeks, months)
        jst = _replace_hour(jst, hour)
        weekday_info = jst.strftime("%A")
        return weekday_info


