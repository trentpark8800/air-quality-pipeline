CREATE TABLE IF NOT EXISTS raw.air_quality (
	location_id BIGINT,
	sensors_id BIGINT,
	"location" VARCHAR,
	"datetime" TIMESTAMP,
	lat DOUBLE,
	lon DOUBLE,
	"parameter" VARCHAR,
	units VARCHAR,
	"value" DOUBLE,
	locationid BIGINT,
	"month" VARCHAR,
	"year" BIGINT,
	ingestion_datetime TIMESTAMP
);