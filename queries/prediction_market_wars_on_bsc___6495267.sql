-- part of a query repo
-- query name: Prediction Market Wars on BSC
-- query link: https://dune.com/queries/6495267


WITH
usdt AS (
  SELECT 0x55d398326f99059fF775485246999027B3197955 AS addr
),
dates AS (
  SELECT d AS day
  FROM UNNEST(SEQUENCE(DATE '2025-10-21', CURRENT_DATE, INTERVAL '1' DAY)) AS t(d)
),

opinion_wallets AS (
  SELECT maker AS user FROM opinion_bnb.ctfexecutionengine_evt_orderfilled
  UNION
  SELECT taker FROM opinion_bnb.ctfexecutionengine_evt_orderfilled
),
opinion_daily_raw AS (
  SELECT DATE_TRUNC('day', block_time) AS day, SUM(amount_usd) AS flow, 'Opinion traders' AS category
  FROM tokens_bnb.transfers
  WHERE contract_address = (SELECT addr FROM usdt)
    AND "to" IN (SELECT user FROM opinion_wallets)
    AND block_number >= 64726315
    AND block_time >= TIMESTAMP '2025-10-21'
  GROUP BY 1,3
  UNION ALL
  SELECT DATE_TRUNC('day', block_time) AS day, -SUM(amount_usd) AS flow, 'Opinion traders' AS category
  FROM tokens_bnb.transfers
  WHERE contract_address = (SELECT addr FROM usdt)
    AND "from" IN (SELECT user FROM opinion_wallets)
    AND block_number >= 64726315
    AND block_time >= TIMESTAMP '2025-10-21'
  GROUP BY 1,3
),
opinion_daily AS (
  SELECT d.day, 'Opinion traders' AS category, COALESCE(r.flow, 0) AS flow
  FROM dates d
  LEFT JOIN opinion_daily_raw r
    ON r.day = d.day
),

predict_wallets AS (
  SELECT maker AS user FROM predict_bnb.ctfexchange_evt_orderfilled
  UNION
  SELECT taker FROM predict_bnb.ctfexchange_evt_orderfilled
  UNION
  SELECT maker FROM predict_bnb.negriskctfexchange_evt_orderfilled
  UNION
  SELECT taker FROM predict_bnb.negriskctfexchange_evt_orderfilled
),
predict_daily AS (
  SELECT DATE_TRUNC('day', block_time) AS day, 'Predict.fun traders' AS category, SUM(amount_usd) AS flow
  FROM tokens_bnb.transfers
  WHERE contract_address = (SELECT addr FROM usdt)
    AND "to" IN (SELECT user FROM predict_wallets)
    AND block_number >= 38737994
    AND block_time >= TIMESTAMP '2025-11-25'
  GROUP BY 1,2
  UNION ALL
  SELECT DATE_TRUNC('day', block_time) AS day, 'Predict.fun traders' AS category, -SUM(amount_usd) AS flow
  FROM tokens_bnb.transfers
  WHERE contract_address = (SELECT addr FROM usdt)
    AND "from" IN (SELECT user FROM predict_wallets)
    AND block_number >= 38737994
    AND block_time >= TIMESTAMP '2025-11-25'
  GROUP BY 1,2
),

probable_wallets AS (
  SELECT BYTEARRAY_SUBSTRING(BYTEARRAY_SUBSTRING(data, 1, 32),13,20) AS user
  FROM bnb.logs
  WHERE topic0 = 0x4f51faf6c4561ff95f067657e43439f0f856d97c04d9ec9070a6199ad418e235
    AND contract_address = 0xB99159aBF0bF59a512970586F38292f8b9029924
),
probable_daily AS (
  SELECT DATE_TRUNC('day', block_time) AS day, 'Probable traders' AS category, SUM(amount_usd) AS flow
  FROM tokens_bnb.transfers
  WHERE contract_address = (SELECT addr FROM usdt)
    AND "to" IN (SELECT user FROM probable_wallets)
    AND block_number >= 38737994
    AND block_time >= TIMESTAMP '2025-12-09'
  GROUP BY 1,2
  UNION ALL
  SELECT DATE_TRUNC('day', block_time) AS day, 'Probable traders' AS category, -SUM(amount_usd) AS flow
  FROM tokens_bnb.transfers
  WHERE contract_address = (SELECT addr FROM usdt)
    AND "from" IN (SELECT user FROM probable_wallets)
    AND block_number >= 38737994
    AND block_time >= TIMESTAMP '2025-12-09'
  GROUP BY 1,2
),

daily AS (
  SELECT * FROM opinion_daily
  UNION ALL
  SELECT * FROM predict_daily
  UNION ALL
  SELECT * FROM probable_daily
)

SELECT
  day,
  category,
  SUM(flow) OVER (PARTITION BY category ORDER BY day) AS "Trader USDT Balance (USD)"
FROM daily
ORDER BY 1,2;