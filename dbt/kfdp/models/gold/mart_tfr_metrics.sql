WITH base AS (
  SELECT
    country,
    year,
    value
  FROM {{ ref('stg_tfr_long') }}
),
metrics AS (
  SELECT
    country,
    year,
    value,
    value - LAG(value) OVER (PARTITION BY country ORDER BY year) AS yoy_delta,
    AVG(value) OVER (
      PARTITION BY country ORDER BY year
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS ma_5y,
    (value < 2.1) AS is_below_replacement
  FROM base
)
SELECT *
FROM metrics
ORDER BY year
