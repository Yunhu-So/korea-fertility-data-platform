WITH src AS (
  SELECT
    country,
    indicator,
    CAST(year AS INTEGER) AS year,
    CAST(value AS DOUBLE) AS value,
    source_file
  FROM {{ source('silver', 'tfr_long') }}
)
SELECT *
FROM src
WHERE indicator = 'TFR'
