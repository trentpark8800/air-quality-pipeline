CREATE OR REPLACE VIEW presentation.daily_air_quality_stats AS
WITH air_quality_cte AS (
    SELECT
        location_id,
        location,
        CAST("datetime" AS DATE) AS measurement_date,
        lat,
        lon,
        parameter,
        units,
        value,
        dayofweek("datetime") AS weekday_number,
        dayname("datetime") AS weekday,
        CASE
            WHEN dayname("datetime") = 'Saturday' OR dayname("datetime") = 'Sunday'
            THEN 1
            ELSE 0
        END AS is_weekend
    FROM presentation.air_quality
)
SELECT
    location_id,
    location,
    measurement_date,
    weekday_number,
    weekday,
    is_weekend,
    lat,
    lon,
    parameter,
    units,
    AVG(value) AS average_value
FROM air_quality_cte
GROUP BY
    location_id,
    location,
    measurement_date,
    weekday_number,
    weekday,
    is_weekend,
    lat,
    lon,
    parameter,
    units;
