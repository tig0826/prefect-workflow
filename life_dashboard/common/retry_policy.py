"""ストレージ起因の一過性エラーに対する共通リトライ方針。

なぜ必要か:
  2026-09-14、NFS サーバ (nas01) が IO 飽和を起こし、MinIO が
  「30秒以内に read+write できない」としてドライブをオフライン判定した。
  その間 Trino は Iceberg のメタデータを読めず、こう返した:

    ICEBERG_INVALID_METADATA: Error accessing metadata file for table
    life_gold.int_aw_web

  データは壊れていなかった。IO が静まったあとに同じクエリを8回投げて
  8回とも成功している。つまり**待てば通るエラー**だった。
  それでも週次FBは死んだ。タスクが `retries=1, retry_delay_seconds=30`
  で、30秒後に1回試して力尽きたからである。

  ストレージの詰まりは数分から十数分続く。HDD 3本の raidz1 に
  MinIO の16ドライブ（NFS 経由）が載っている構成である以上、
  重い分析が走れば IO は飽和しうるし、それは異常ではなく仕様上の帯域不足。
  「30秒後に1回」はこの現実に対して短すぎる。

方針:
  1. 固定遅延ではなく指数バックオフで、合計15分ほど粘る。
     30s -> 60s -> 120s -> 240s -> 480s (+ジッタ)。
     今回の詰まりは実質3分程度だったので、これで完走できたはず。

  2. ジッタを入れる。複数タスクが同時に落ちたとき、同じ秒数で揃って
     再突入すると、回復しかけたストレージをまた潰す。

  3. ただし `TrinoUserError`（SQL構文ミス、テーブル名やカラム名の
     打ち間違いなど）は何度試しても結果が変わらないので即座に失敗させる。
     これをやらないと typo の確認に15分待たされ、開発が止まる。
     Trino はエラーを USER / EXTERNAL / INTERNAL / INSUFFICIENT_RESOURCES
     に分類しており、例外クラスがそのまま使える。今回の
     ICEBERG_INVALID_METADATA は EXTERNAL だった。

使い方:
    from common.retry_policy import STORAGE_RETRY

    @task(name="Fetch something", **STORAGE_RETRY)
    def fetch_something(): ...

  LLM 呼び出しなどストレージ以外のタスクには付けない（別の理由で落ちるので
  別の方針が要る）。
"""

import logging

from prefect.tasks import exponential_backoff

log = logging.getLogger(__name__)

# 1回目の待ち時間。以降 2倍ずつ伸びる。
BACKOFF_FACTOR_SECONDS = 30
# 試行回数（初回を除く）。30+60+120+240+480 = 930秒 ≒ 15.5分粘る。
MAX_RETRIES = 5
# 待ち時間に足す揺らぎの割合。同時に落ちたタスクの再突入をばらけさせる。
JITTER_FACTOR = 0.3


def _is_deterministic(exc: BaseException) -> bool:
    """何度リトライしても同じ結果になるエラーか。

    True を返したものはリトライせず即座に失敗させる。
    """
    # Trino: USER_ERROR は SQL 側の誤り。待っても直らない。
    try:
        from trino.exceptions import TrinoUserError

        if isinstance(exc, TrinoUserError):
            return True
    except ImportError:
        pass

    # MinIO/S3: バケット不在や権限不足は待っても直らない。
    # 一方 SlowDown / RequestTimeout / 5xx は混雑なのでリトライ対象。
    try:
        from botocore.exceptions import ClientError

        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            return code in {
                "NoSuchBucket",
                "NoSuchKey",
                "AccessDenied",
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "InvalidBucketName",
            }
    except ImportError:
        pass

    return False


def retry_unless_deterministic(task, task_run, state) -> bool:
    """Prefect の retry_condition_fn。リトライすべきなら True。

    判定できない例外はリトライする側に倒す。ストレージの不調は
    想定外の形でも現れるので、未知のものを握り潰さないほうが安全。
    """
    try:
        state.result(raise_on_failure=True)
    except BaseException as exc:  # noqa: BLE001 - 例外の種類で分岐するのが目的
        if _is_deterministic(exc):
            log.warning(
                "%s: リトライしても直らないエラーなので即座に失敗させます: %s",
                getattr(task, "name", "task"),
                exc,
            )
            return False
        return True
    # 失敗状態なのに例外が取れないケース。念のためリトライ側に倒す。
    return True


STORAGE_RETRY = {
    "retries": MAX_RETRIES,
    "retry_delay_seconds": exponential_backoff(backoff_factor=BACKOFF_FACTOR_SECONDS),
    "retry_jitter_factor": JITTER_FACTOR,
    "retry_condition_fn": retry_unless_deterministic,
}
