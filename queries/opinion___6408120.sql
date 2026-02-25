-- part of a query repo
-- query name: Opinion
-- query link: https://dune.com/queries/6408120


WITH opinion AS (
  SELECT
    LOWER(REGEXP_REPLACE(condition_id, '^0x', '')) AS condition_id_hex,
    CASE
      WHEN POSITION(' - ' IN COALESCE(NULLIF(title_short, ''), NULLIF(title, ''))) > 0
        THEN SPLIT_PART(COALESCE(NULLIF(title_short, ''), NULLIF(title, '')), ' - ', 1)
      ELSE COALESCE(NULLIF(title_short, ''), NULLIF(title, ''))
    END AS market_name,
    create_time,
    ROW_NUMBER() OVER (
      PARTITION BY LOWER(REGEXP_REPLACE(condition_id, '^0x', ''))
      ORDER BY create_time DESC
    ) AS rn
  FROM dune.opinionlabs.opinion_markets_metadata_56
  WHERE condition_id IS NOT NULL
),

condition_map AS (
  SELECT
    condition_id_hex,
    market_name,
    create_time
  FROM opinion
  WHERE rn = 1
),

token_map_raw AS (
  SELECT
    CAST(b.token0 AS VARCHAR) AS token,
    cm.market_name,
    cm.create_time
  FROM opinion_bnb.ctfexecutionengine_evt_tokenregistered b
  JOIN condition_map cm
    ON ('0x' || cm.condition_id_hex) = CAST(b.conditionId AS VARCHAR)

  UNION ALL

  SELECT
    CAST(b.token1 AS VARCHAR) AS token,
    cm.market_name,
    cm.create_time
  FROM opinion_bnb.ctfexecutionengine_evt_tokenregistered b
  JOIN condition_map cm
    ON ('0x' || cm.condition_id_hex) = CAST(b.conditionId AS VARCHAR)
),

token_map AS (
  SELECT
    token,
    market_name
  FROM (
    SELECT
      token,
      market_name,
      ROW_NUMBER() OVER (PARTITION BY token ORDER BY create_time DESC) AS rn
    FROM token_map_raw
  )
  WHERE rn = 1
),

fills AS (
  SELECT
    CAST(DATE_TRUNC('day', evt_block_time) AS date) AS day,
    evt_tx_hash,
    CAST(maker AS VARCHAR) AS maker,
    CAST(taker AS VARCHAR) AS taker,
    CAST(makerAssetId AS VARCHAR) AS makerAssetId,
    CAST(takerAssetId AS VARCHAR) AS takerAssetId,
    makerAmountFilled,
    takerAmountFilled,
    CASE
      WHEN CAST(makerAssetId AS VARCHAR) = '0' THEN CAST(takerAssetId AS VARCHAR)
      WHEN CAST(takerAssetId AS VARCHAR) = '0' THEN CAST(makerAssetId AS VARCHAR)
      ELSE NULL
    END AS outcome_token
  FROM opinion_bnb.ctfexecutionengine_evt_orderfilled
  WHERE evt_block_date >= DATE '2025-01-01'
),

labeled AS (
  SELECT
    f.day,
    tm.market_name AS market,
    f.evt_tx_hash,
    f.maker,
    f.taker,
    CASE
      WHEN f.makerAssetId = '0' THEN CAST(f.makerAmountFilled AS DOUBLE) / 1e18
      WHEN f.takerAssetId = '0' THEN CAST(f.takerAmountFilled AS DOUBLE) / 1e18
      ELSE NULL
    END AS notional,
    CASE
      WHEN f.makerAssetId = '0' THEN CAST(f.makerAmountFilled AS DOUBLE) / 1e18 / 2
      WHEN f.takerAssetId = '0' THEN CAST(f.takerAmountFilled AS DOUBLE) / 1e18 / 2
      ELSE NULL
    END AS volume
  FROM fills f
  JOIN token_map tm
    ON f.outcome_token = tm.token
  WHERE f.outcome_token IS NOT NULL
    AND tm.market_name IS NOT NULL
),

daily_core AS (
  SELECT
    day,
    market,
    SUM(notional) AS daily_notional,
    SUM(volume) AS daily_volume
  FROM labeled
  GROUP BY 1, 2
),

daily_trx AS (
  SELECT
    day,
    market,
    COUNT(DISTINCT evt_tx_hash) AS daily_trades
  FROM labeled
  GROUP BY 1, 2
),

daily_users AS (
  SELECT day, market, maker AS address FROM labeled
  UNION
  SELECT day, market, taker AS address FROM labeled
),

daily_user_counts AS (
  SELECT
    day,
    market,
    COUNT(DISTINCT address) AS daily_users
  FROM daily_users
  GROUP BY 1, 2
)

SELECT
  c.day AS "Day",
  c.market AS "Market",
  c.daily_notional AS "Notional Volume",
  c.daily_volume AS "Volume",
  t.daily_trades AS "Daily Trades",
  COALESCE(u.daily_users, 0) AS "Daily Users"
FROM daily_core c
JOIN daily_trx t
  ON c.day = t.day AND c.market = t.market
LEFT JOIN daily_user_counts u
  ON c.day = u.day AND c.market = u.market
ORDER BY "Day" DESC, "Notional Volume" DESC;
