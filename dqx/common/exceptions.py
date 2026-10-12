class SessionExpiredException(Exception):
    """セッション（Cookie）が切れてログイン画面に飛ばされたことを示す例外"""

    pass


class SiteMaintenanceException(Exception):
    """冒険者の広場がメンテナンス中で価格を取得できないことを示す例外"""

    pass
