"""総当たり探索のための統計ユーティリティ。

なぜ必要か:
  「予測できない問題を拾う」には関係の型を機械的に全部試す必要がある。
  しかし探索空間は cat_sub 33種 × 24時間 = 792セル（× source 4 で最大3,168）あり、
  素朴に p<0.05 で検定すると**偽陽性が約158件**出る。
  それを「発見」として LLM に渡すと、ai_feedback_flow の `_correlation` が
  n>=4 で「傾向がある」と言っていたのと同じ捏造を、より大きな規模で再発させる。

  そこで「多数の候補から生き残ったものだけを渡す」層をここに置く。
  **LLM に検定させない**（必ず盛るので）。

標準ライブラリだけで完結させている（scipy を入れない）。
"""

import math

# ── 正規分布 ────────────────────────────────────────────────
def _norm_sf(z: float) -> float:
    """標準正規分布の上側確率。erfc で正確に出せる。"""
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def two_sided_z_p(z: float) -> float:
    return min(1.0, 2.0 * _norm_sf(abs(z)))


# ── t 分布（相関の検定用）──────────────────────────────────
def _t_sf(t: float, df: int) -> float:
    """t 分布の上側確率。正則不完全ベータ関数の連分数展開で計算する。

    scipy を使わずに済ませるため。df>=1 で実用精度が出る。
    """
    if df <= 0:
        return 1.0
    x = df / (df + t * t)
    # I_x(df/2, 1/2) / 2 が上側確率（t>0 のとき）
    p = 0.5 * _betainc(df / 2.0, 0.5, x)
    return p if t > 0 else 1.0 - p


def _betainc(a: float, b: float, x: float) -> float:
    """正則不完全ベータ関数 I_x(a,b)。Lentz 法の連分数。"""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    lbeta = math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)
    front = math.exp(math.log(x) * a + math.log(1.0 - x) * b - lbeta)
    # 収束を良くするため対称性を使う
    if x > (a + 1.0) / (a + b + 2.0):
        return 1.0 - _betainc(b, a, 1.0 - x)
    f, c, d = 1.0, 1.0, 0.0
    for i in range(0, 300):
        m = i // 2
        if i == 0:
            num = 1.0
        elif i % 2 == 0:
            num = (m * (b - m) * x) / ((a + 2.0 * m - 1.0) * (a + 2.0 * m))
        else:
            num = -((a + m) * (a + b + m) * x) / ((a + 2.0 * m) * (a + 2.0 * m + 1.0))
        d = 1.0 + num * d
        if abs(d) < 1e-30:
            d = 1e-30
        d = 1.0 / d
        c = 1.0 + num / c
        if abs(c) < 1e-30:
            c = 1e-30
        f *= c * d
        if abs(1.0 - c * d) < 1e-10:
            break
    return front * (f - 1.0) / a


def pearson_with_p(xs: list, ys: list, min_pairs: int = 10) -> dict | None:
    """ピアソン相関と両側 p 値。ペア数が足りなければ None。

    ai_feedback_flow._correlation は臨界値テーブルで判定しているが、
    こちらは p 値を返して FDR 制御にかけられるようにする。
    """
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    n = len(pairs)
    if n < min_pairs:
        return None
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    dx = math.sqrt(sum((p[0] - mx) ** 2 for p in pairs))
    dy = math.sqrt(sum((p[1] - my) ** 2 for p in pairs))
    if dx == 0 or dy == 0:
        return None
    r = num / (dx * dy)
    r = max(-0.999999, min(0.999999, r))
    t = r * math.sqrt((n - 2) / (1 - r * r))
    p = 2.0 * _t_sf(abs(t), n - 2)
    return {"r": round(r, 3), "n": n, "p": p}


# ── 多重検定の制御 ──────────────────────────────────────────
def benjamini_hochberg(items: list[dict], alpha: float = 0.05,
                       p_key: str = "p") -> tuple[list[dict], dict]:
    """BH 法で FDR を制御し、生き残った候補と要約を返す。

    各 item に `q`（調整後 p 値）と `significant` を付ける。
    返り値の要約には「素朴に alpha で切った場合の偽陽性期待値」も入れて、
    どれだけ絞られたかを人間が確認できるようにする。
    """
    scored = [it for it in items if it.get(p_key) is not None]
    m = len(scored)
    if m == 0:
        return [], {"tested": 0, "survived": 0, "alpha": alpha}

    ordered = sorted(scored, key=lambda x: x[p_key])
    # BH: 最大の k で p_k <= k/m*alpha を満たすものまでを有意とする
    k_max = 0
    for i, it in enumerate(ordered, start=1):
        if it[p_key] <= i / m * alpha:
            k_max = i

    # q 値（単調化した調整後 p 値）
    q_prev = 1.0
    for i in range(m, 0, -1):
        it = ordered[i - 1]
        q = min(q_prev, it[p_key] * m / i)
        it["q"] = round(min(1.0, q), 4)
        q_prev = q

    for i, it in enumerate(ordered, start=1):
        it["significant"] = i <= k_max

    survivors = [it for it in ordered if it["significant"]]
    summary = {
        "tested": m,
        "survived": len(survivors),
        "alpha": alpha,
        "naive_expected_false_positives": round(m * alpha, 1),
        "note": (
            f"{m}件を検定し {len(survivors)}件が BH-FDR({alpha}) を通過。"
            f"素朴に p<{alpha} で切ると偽陽性が期待値 {round(m * alpha, 1)}件出るため、"
            "生き残った候補だけを扱う。"
        ),
    }
    return survivors, summary


def poisson_like_p(observed: float, expected: float) -> float | None:
    """観測が期待値からどれだけ外れているかの両側 p 値（正規近似）。

    分単位の重なり時間のように「独立なら期待値 E」という帰無仮説を置ける量に使う。
    E が小さいと正規近似が粗くなるので、下流で最低量のフィルタをかける前提。
    """
    if expected is None or expected <= 0:
        return None
    z = (observed - expected) / math.sqrt(expected)
    return two_sided_z_p(z)
