-- part of a query repo
-- query name: Prediction Market Wars on BSC
-- query link: https://dune.com/queries/6388840


WITH fees AS (
  SELECT
    "Day",
    'Predict.Fun' AS platform,
    "Daily Fee",
    "Cumulative Fee"
  FROM query_6367997

  UNION ALL

  SELECT
    "Day",
    'Opinion' AS platform,
    "Daily Fee",
    "Cumulative Fee"
  FROM query_6388877

  UNION ALL

  SELECT
    "Day",
    'Probable' AS platform,
    "Daily Fee",
    "Cumulative Fee"
  FROM query_6388916
)

SELECT
  "Day",
  platform,
  "Daily Fee",
  "Cumulative Fee"
FROM fees
WHERE "Day" < CURRENT_DATE
ORDER BY "Day" DESC, platform;
