CREATE OR REPLACE VIEW presentation.latest_param_values_per_location AS
WITH ranked_data AS (
  SELECT
    location_id,
    location,
    lat,
    lon,
    parameter,
    value,
    datetime,
    ROW_NUMBER() OVER (PARTITION BY location_id, parameter ORDER BY datetime DESC) AS rn
  FROM
    presentation.air_quality
)
PIVOT (
	SELECT
		location_id,
	    location,
	    lat,
	    lon,
	    parameter,
	    value,
	    datetime
	FROM ranked_data
	WHERE rn = 1
)
ON parameter IN ('pm10', 'pm25', 'so2')
USING FIRST("value");