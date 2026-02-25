-- part of a query repo
-- query name: Probable
-- query link: https://dune.com/queries/6447097


WITH markets_page1 AS (
  SELECT try_cast(
    json_parse(
      http_get('https://market-api.probable.markets/public/api/v1/markets/?page=1&limit=100')
    ) AS json
  ) AS resp
),
markets_total_pages AS (
  SELECT CAST(json_extract_scalar(resp, '$.pagination.totalPages') AS integer) AS total_pages
  FROM markets_page1
),
pages AS (
  SELECT p AS page
  FROM markets_total_pages
  CROSS JOIN UNNEST(sequence(1, total_pages)) AS t(p)
),
markets_pages_json AS (
  SELECT
    page,
    try_cast(
      json_parse(
        http_get(
          concat(
            'https://market-api.probable.markets/public/api/v1/markets/?limit=100&page=',
            CAST(page AS varchar)
          )
        )
      ) AS json
    ) AS resp
  FROM pages
),
markets_unnested AS (
  SELECT market
  FROM markets_pages_json
  CROSS JOIN UNNEST(CAST(json_extract(resp, '$.markets') AS array(json))) AS t(market)
),
markets_q AS (
  SELECT
    lpad(
      regexp_replace(lower(json_extract_scalar(market, '$.condition_id')), '^0x', ''),
      64,
      '0'
    ) AS condition_id_hex,
    json_extract_scalar(market, '$.question') AS question
  FROM markets_unnested
),

token_reg AS (
  SELECT
    evt_block_number,
    lpad(lower(regexp_replace(to_hex(conditionId), '^0x', '')), 64, '0') AS condition_id_hex,
    CAST(token0 AS uint256) AS token_id
  FROM probable_v1_bnb.ctfexchange_evt_tokenregistered
  UNION ALL
  SELECT
    evt_block_number,
    lpad(lower(regexp_replace(to_hex(conditionId), '^0x', '')), 64, '0') AS condition_id_hex,
    CAST(token1 AS uint256) AS token_id
  FROM probable_v1_bnb.ctfexchange_evt_tokenregistered
),
outcome_tokens AS (
  SELECT DISTINCT token_id FROM token_reg
),

fills AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    evt_block_number,
    evt_index,
    evt_tx_hash,
    CAST(makerAmountFilled AS DOUBLE) AS maker_amt_raw,
    CAST(takerAmountFilled AS DOUBLE) AS taker_amt_raw,
    CAST(makerAssetId AS uint256) AS maker_asset,
    CAST(takerAssetId AS uint256) AS taker_asset,
    maker,
    taker
  FROM probable_v1_bnb.ctfexchange_evt_orderfilled
),

sided AS (
  SELECT
    f.day,
    f.evt_block_number,
    f.evt_index,
    f.evt_tx_hash,
    f.maker,
    f.taker,
    CASE
      WHEN om.token_id IS NOT NULL AND ot.token_id IS NULL THEN f.maker_asset
      WHEN ot.token_id IS NOT NULL AND om.token_id IS NULL THEN f.taker_asset
      ELSE NULL
    END AS token_id,
    CASE
      WHEN om.token_id IS NOT NULL AND ot.token_id IS NULL THEN (f.taker_amt_raw / 1e18)
      WHEN ot.token_id IS NOT NULL AND om.token_id IS NULL THEN (f.maker_amt_raw / 1e18)
      ELSE NULL
    END AS notional,
    CASE
      WHEN om.token_id IS NOT NULL AND ot.token_id IS NULL THEN (f.taker_amt_raw / 1e18) / 2
      WHEN ot.token_id IS NOT NULL AND om.token_id IS NULL THEN (f.maker_amt_raw / 1e18) / 2
      ELSE NULL
    END AS volume
  FROM fills f
  LEFT JOIN outcome_tokens om ON f.maker_asset = om.token_id
  LEFT JOIN outcome_tokens ot ON f.taker_asset = ot.token_id
  WHERE
    (om.token_id IS NOT NULL AND ot.token_id IS NULL)
    OR (ot.token_id IS NOT NULL AND om.token_id IS NULL)
),

token_condition_at_trade AS (
  SELECT
    s.evt_tx_hash,
    s.evt_index,
    s.token_id,
    r.condition_id_hex,
    row_number() OVER (
      PARTITION BY s.evt_tx_hash, s.evt_index, s.token_id
      ORDER BY r.evt_block_number DESC
    ) AS rn
  FROM sided s
  JOIN token_reg r
    ON s.token_id = r.token_id
   AND r.evt_block_number <= s.evt_block_number
),

with_condition AS (
  SELECT
    s.day,
    t.condition_id_hex,
    s.notional,
    s.volume,
    s.maker,
    s.taker,
    s.evt_tx_hash
  FROM sided s
  LEFT JOIN (
    SELECT evt_tx_hash, evt_index, token_id, condition_id_hex
    FROM token_condition_at_trade
    WHERE rn = 1
  ) t
    ON s.evt_tx_hash = t.evt_tx_hash
   AND s.evt_index = t.evt_index
   AND s.token_id = t.token_id
),

with_question AS (
  SELECT
    w.day,
    q.question,
    w.notional,
    w.volume,
    w.maker,
    w.taker,
    w.evt_tx_hash
  FROM with_condition w
  JOIN markets_q q
    ON w.condition_id_hex = q.condition_id_hex
),

daily_core AS (
  SELECT
    day,
    question,
    SUM(notional) AS daily_notional,
    SUM(volume) AS daily_volume
  FROM with_question
  GROUP BY 1,2
),

daily_trx AS (
  SELECT
    day,
    question,
    COUNT(DISTINCT evt_tx_hash) AS daily_trades
  FROM with_question
  GROUP BY 1,2
),

daily_users_exploded AS (
  SELECT day, question, lower(regexp_replace(to_hex(maker), '^0x', '')) AS address FROM with_question
  UNION ALL
  SELECT day, question, lower(regexp_replace(to_hex(taker), '^0x', '')) AS address FROM with_question
),

daily_user_counts AS (
  SELECT
    day,
    question,
    COUNT(DISTINCT address) AS daily_users
  FROM daily_users_exploded
  GROUP BY 1,2
)

SELECT
  c.day AS "Day",
  c.question AS "Question",
  c.daily_notional AS "Notional Volume",
  c.daily_volume AS "Volume",
  t.daily_trades AS "Daily Trades",
  u.daily_users AS "Daily Users"
FROM daily_core c
LEFT JOIN daily_trx t
  ON c.day = t.day AND c.question = t.question
LEFT JOIN daily_user_counts u
  ON c.day = u.day AND c.question = u.question
ORDER BY "Day" DESC, "Notional Volume" DESC;
