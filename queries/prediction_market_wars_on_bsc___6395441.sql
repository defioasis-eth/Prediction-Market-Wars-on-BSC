-- part of a query repo
-- query name: Prediction Market Wars on BSC
-- query link: https://dune.com/queries/6395441


WITH bsc AS (
    SELECT DATE '2025-10-15' AS start_date
),
bscpredict AS (
    SELECT CAST(day AS date) AS day, 'Predict.Fun' AS platform, CAST(tvl AS double) AS tvl
    FROM query_6394223, bsc
    WHERE CAST(day AS date) >= bsc.start_date

    UNION ALL

    SELECT CAST(day AS date) AS day, 'Opinion' AS platform, CAST(tvl AS double) AS tvl
    FROM query_6395228, bsc
    WHERE CAST(day AS date) >= bsc.start_date

    UNION ALL

    SELECT CAST(day AS date) AS day, 'Probable' AS platform, CAST(tvl AS double) AS tvl
    FROM query_6425860, bsc
    WHERE CAST(day AS date) >= bsc.start_date
)
SELECT
    day,
    platform,
    tvl,
    tvl - LAG(tvl) OVER (PARTITION BY platform ORDER BY day) AS tvl_delta
FROM bscpredict
ORDER BY day DESC, platform;
