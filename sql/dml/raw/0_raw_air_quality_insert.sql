INSERT INTO raw.air_quality
SELECT 
    location_id, 
    sensors_id, 
    "location", 
    "datetime", 
    lat, 
    lon, 
    "parameter", 
    units, 
    "value", 
    locationid, 
    "month", 
    "year",
    current_timestamp AS ingestion_datetime
FROM read_csv('{{ data_file_path }}');