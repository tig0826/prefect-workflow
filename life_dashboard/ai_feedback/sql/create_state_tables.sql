-- AI フィードバックが「状態」を持つためのテーブル群。
--
-- なぜ必要か:
--   従来の日次FBは状態を持たず、毎日ゼロから同じ入力を見て同じ結論に到達していた
--   （直近30日70件の実測で「深夜」言及 81%、「概日リズム」49%）。
--   プロンプトに「繰り返すな」と書いても原理的に効かない。生活習慣は日次では
--   変化しないので、正しく分析するほど結論は同じになる。
--
--   解決は「FB ができることを構造的に制限する」こと:
--     ・新しい issue を立てる
--     ・既存 issue の metric がどう動いたか報告する
--   のどちらかしかできないようにする。mention_count が閾値を超えて metric が
--   動いていなければ「この仮説は外れた」を機械的に強制できる。
--
-- 3テーブルの役割分担:
--   ai_feedback_issues    … 課題と仮説。metric_sql で自動評価できる形に強制する
--   ai_interventions      … 実際に打った手。issue と紐付けて前後比較の基準日になる
--   ai_metric_history     … metric の時系列。これが無いと「効いたか」を言えない

-- ─────────────────────────────────────────────────────────────
-- 1. 課題と仮説
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.life_gold.ai_feedback_issues (
    issue_id VARCHAR,
    opened_date DATE,

    title VARCHAR,
    -- 症状ではなく原因を書く。例:
    --   × 「昼寝が長い」        （症状。削っても総睡眠が減るだけ）
    --   ○ 「深夜0-3時の開発で主睡眠が238分に縮み、その不足を昼寝で補填している」
    hypothesis VARCHAR,

    -- どの機構が見つけたか。発見率の内訳を測るために必須。
    -- 'structural_overlap' | 'structural_absence' | 'structural_transition'
    -- | 'anomaly' | 'weekly_llm' | 'completeness_critic' | 'manual'
    discovered_by VARCHAR,
    -- 起票時の根拠数値（JSON文字列）
    evidence VARCHAR,

    -- ★ここが設計の中核★
    -- 自動評価用の SQL。「1行1列の数値」を返す契約。
    -- これを書けない処方は出力させない = 「十分な睡眠を」「無理は禁物」が
    -- 機械的に落ちる。metric を書けるものだけが処方として通る。
    metric_sql VARCHAR,
    metric_name VARCHAR,
    metric_unit VARCHAR,
    baseline_value DOUBLE,
    target_value DOUBLE,
    -- 'decrease' | 'increase'
    target_direction VARCHAR,

    -- 'open' | 'testing' | 'resolved' | 'abandoned' | 'rejected'
    --   testing   … 介入を打って効果待ち
    --   abandoned … 仮説が外れたと判定（mention_count 超過かつ metric 不動）
    --   rejected  … 検定を通らず偽陽性と判定
    status VARCHAR,

    -- 何回言及したか。閾値を超えて metric が動かなければ仮説を棄却させる。
    mention_count INTEGER,
    last_mentioned_date DATE,
    resolved_date DATE,
    notes VARCHAR,

    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6)
) WITH (
    format = 'parquet'
);

-- ─────────────────────────────────────────────────────────────
-- 2. 実際に打った手
-- ─────────────────────────────────────────────────────────────
-- 効果測定が成立する条件は「介入がコードとして存在し、結果がログとして
-- 自動で溜まること」。AdGuard のブロック変更が測れたのはそれが満たされていたから。
-- ユーザーに「試しましたか？」と聞く設計にすると必ず続かないので聞かない。
CREATE TABLE IF NOT EXISTS iceberg.life_gold.ai_interventions (
    intervention_id VARCHAR,
    -- 紐付く issue。単独の介入（探索的な試行）なら NULL 可
    issue_id VARCHAR,

    started_at TIMESTAMP(6),
    -- NULL = 現在も継続中
    ended_at TIMESTAMP(6),

    -- 'dns_block' | 'schedule' | 'environment' | 'behavior' | 'instrumentation' | 'other'
    kind VARCHAR,
    description VARCHAR,
    -- 再現性のための参照。例: 'ansible@6df2b77 playbooks/adguard-blocks.yml'
    config_ref VARCHAR,

    -- 'human' | 'weekly_llm' | 'manual'
    created_by VARCHAR,
    notes VARCHAR,

    created_at TIMESTAMP(6)
) WITH (
    format = 'parquet'
);

-- ─────────────────────────────────────────────────────────────
-- 3. metric の時系列
-- ─────────────────────────────────────────────────────────────
-- 毎朝 metric_sql を全 open/testing issue に対して実行して1行積む。
-- 「同じことを100回言い続けても誰も気づけない」構造を壊すための土台。
CREATE TABLE IF NOT EXISTS iceberg.life_gold.ai_metric_history (
    issue_id VARCHAR,
    eval_date DATE,
    metric_value DOUBLE,
    -- 介入が実際に守られていたかを metric から自動判定した割合。
    -- 本人に聞かずに出せるものだけ入れる。出せない場合は NULL。
    adherence_pct DOUBLE,
    -- 評価が失敗した場合の理由（SQL エラー、データ欠損など）。
    -- 欠損を「改善」と誤読しないために必ず残す。
    eval_error VARCHAR,
    evaluated_at TIMESTAMP(6)
) WITH (
    format = 'parquet',
    partitioning = ARRAY['eval_date']
);

-- ─────────────────────────────────────────────────────────────
-- 4. チャット発言の追記専用ログ
-- ─────────────────────────────────────────────────────────────
-- なぜ必要か:
--   「調子の悪さ」を追える受動指標が現時点で存在しない（HRV等は睡眠中計測で
--   カバレッジ55%かつ悪い夜に落ちる MNAR、HR床は何とも有意な相関がない）。
--   一方チャットには主観状態の記述が本人の労力ゼロで溜まっており、
--   しかも調子が悪いほど書く量が増えるので MNAR が有利な方向に働く。
--
--   ところが life_gold.chat_history は `DELETE WHERE 1=1` → INSERT の全上書きで、
--   さらに直近100件に切り捨てていた。**ラベル源が消え続けていた。**
--
--   ここは追記専用にして消さない。message_id で冪等にする。
--
-- 注意: message_ts は「保存された時刻」で、発言時刻そのものではない
--   （POST はやり取り直後に走るので実用上は近い）。
--   ts_source='backfill' の行は旧 chat_history から復元したもので時刻が不正確なため、
--   時刻に依存する分析からは除外する。
CREATE TABLE IF NOT EXISTS iceberg.life_gold.chat_messages (
    message_id VARCHAR,
    message_ts TIMESTAMP(6),
    chat_date DATE,
    role VARCHAR,
    text VARCHAR,
    -- 'live'（保存時に記録）| 'backfill'（旧テーブルから復元・時刻不正確）
    ts_source VARCHAR,
    created_at TIMESTAMP(6)
) WITH (
    format = 'parquet',
    partitioning = ARRAY['chat_date']
);

-- ─────────────────────────────────────────────────────────────
-- 5. 計装（新しく取るべきデータ）の提案
-- ─────────────────────────────────────────────────────────────
-- なぜ必要か:
--   「取るべきデータ」を人（私やユーザー）の思いつきで決めると、捨てたはずの
--   「ハードコードされた検出器3種」と同じ問題を計装レイヤーで再発させる。
--   そこで完全性クリティックが「分析が詰まった箇所」から毎週生成する形にした。
--
--   ところが提案が context_summary に埋まるだけでは追跡できない。
--   実際に初回で「HRV を Fitbit API から取得して統合すべき」と提案されたが、
--   それは既に実装済みで、原因は週次コンテキストへの配線漏れだった。
--   **同じ提案が毎週出ても気づけない**ので状態を持たせる。
--
-- status の使い方:
--   proposed          … 提案されたが未判断
--   already_satisfied … 既に実装済み（配線漏れ等が原因で提案された）
--   accepted          … やると決めた
--   implemented       … 実装した
--   rejected          … やらないと決めた（理由を notes に残す）
CREATE TABLE IF NOT EXISTS iceberg.life_gold.ai_instrumentation_proposals (
    proposal_id VARCHAR,
    -- 同じ提案を束ねるためのキー（question を正規化したハッシュ）。
    -- これで重複提案を検出し、proposal_count を積む。
    dedup_key VARCHAR,
    first_proposed_date DATE,
    last_proposed_date DATE,
    proposal_count INTEGER,

    -- 答えられなかった問い
    question VARCHAR,
    missing_data VARCHAR,
    -- 本人の手を借りずに取る方法。手入力を要するものは提案させない方針
    -- （調子が悪い日ほど記録が飛ぶ MNAR になり、最も欲しいデータだけ欠ける）
    how_to_collect_passively VARCHAR,
    -- 取れたら何が判定できるようになるか。これが書けない提案は採用しない
    would_enable VARCHAR,

    status VARCHAR,
    notes VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6)
) WITH (
    format = 'parquet'
);
