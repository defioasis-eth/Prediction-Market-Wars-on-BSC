-- part of a query repo
-- query name: Prediction Market Wars on BSC
-- query link: https://dune.com/queries/6374395


WITH bscpredict AS (
  SELECT
    "Day",
    'Predict.Fun' AS platform,
    "Notional Volume",
    "Cumulative Notional Volume",
    "Volume",
    "Cumulative Volume",
    "Daily Trades",
    "Cumulative Trades",
    "Daily Users",
    "Cumulative Users"
  FROM query_6367871

  UNION ALL

  SELECT
    "Day",
    'Probable' AS platform,
    "Notional Volume",
    "Cumulative Notional Volume",
    "Volume",
    "Cumulative Volume",
    "Daily Trades",
    "Cumulative Trades",
    "Daily Users",
    "Cumulative Users"
  FROM query_6373398

  UNION ALL

  SELECT
    "Day",
    'Opinion' AS platform,
    "Notional Volume",
    "Cumulative Notional Volume",
    "Volume",
    "Cumulative Volume",
    "Daily Trades",
    "Cumulative Trades",
    "Daily Users",
    "Cumulative Users"
  FROM query_6374368

  UNION ALL

  SELECT
    "Day",
    'Myriad' AS platform,
    "Notional Volume",
    "Cumulative Notional Volume",
    "Volume",
    "Cumulative Volume",
    "Daily Trades",
    "Cumulative Trades",
    "Daily Users",
    "Cumulative Users"
  FROM query_6471545
)

SELECT
  "Day",
  platform,
  "Notional Volume",
  "Cumulative Notional Volume",
  "Volume",
  "Cumulative Volume",
  "Daily Trades",
  "Cumulative Trades",
  "Daily Users",
  "Cumulative Users"
FROM bscpredict
WHERE "Day" < CURRENT_DATE
ORDER BY "Day" DESC, platform;
