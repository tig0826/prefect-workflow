# keepalive.py
import os, time, random, re
import httpx
from prefect import flow, task, get_run_logger

APP_URL = "https://dqx-kishoukaku.streamlit.app/"

_ASSET_RE = re.compile(r'''src="/-/build/assets/([^"]+\.js)"|href="/-/build/assets/([^"]+\.css)"''')

def _fetch(url: str, method="GET", headers=None, timeout=30.0, follow_redirects=True):
    with httpx.Client(headers=headers or {}, timeout=timeout, follow_redirects=follow_redirects) as c:
        return (c.head if method == "HEAD" else c.get)(url)

@task(retries=5, retry_delay_seconds=30, log_prints=True)
def warm_streamlit(url: str, asset_prefetch: bool = True):
    logger = get_run_logger()
    # 軽い HEAD → 必要なら GET。どちらもキャッシュ抑止・ジッタ付き。
    time.sleep(random.uniform(0, 2.0))
    headers = {
        "User-Agent": "Mozilla/5.0 (prefect-keepalive)",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": url,
    }

    # 1) HEAD（安い起床トリガ）
    try:
        r_head = _fetch(url, method="HEAD", headers=headers, timeout=10.0)
        if 200 <= r_head.status_code < 400:
            logger.info(f"HEAD {url} -> {r_head.status_code}")
    except Exception as e:
        logger.info(f"HEAD {url} failed: {e}")

    # 2) GET（HTML 本体を確実に返させる）
    buster = f"?_={int(time.time())}-{random.randint(0, 1_000_000)}"
    t0 = time.perf_counter()
    r = _fetch(url + buster, headers=headers)
    print(r.content)
    dt = time.perf_counter() - t0
    ok = 200 <= r.status_code < 400 and b"<div id=\"root\"></div>" in r.content
    logger.info(f"GET  {r.request.url} -> {r.status_code} in {dt:.2f}s ok={ok}")

    if not (200 <= r.status_code < 400):
        raise RuntimeError(f"bad status: {r.status_code}")

    # 3) 主要バンドルを 1〜2 本だけ先取り（CDN/キャッシュのウォームアップ）
    if asset_prefetch:
        html = r.text
        hits = _ASSET_RE.findall(html)
        assets = []
        for js, css in hits:
            if js:
                assets.append(f"/-/build/assets/{js}")
            if css:
                assets.append(f"/-/build/assets/{css}")
            if len(assets) >= 2:
                break
        for a in assets:
            asset_url = url.rstrip("/") + a
            try:
                ra = _fetch(asset_url, headers=headers, timeout=15.0)
                print(ra.content)
                logger.info(f"ASSET {asset_url} -> {ra.status_code}")
            except Exception as e:
                logger.info(f"ASSET {asset_url} failed: {e}")

    return {"status": r.status_code, "elapsed": dt, "ok": ok}

@flow(name="keepalive_streamlit")
def keepalive_streamlit():
    return warm_streamlit(APP_URL, asset_prefetch=True)
