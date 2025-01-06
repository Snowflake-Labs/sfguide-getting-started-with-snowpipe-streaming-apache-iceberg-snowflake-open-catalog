-- ====================
-- SETUP
-- ====================

use role ACCOUNTADMIN;

create database if not exists DASH_DB;
create schema if not exists RAW;
create schema if not exists REFINED;

create or replace ICEBERG TABLE DASH_DB.RAW.VEHICLE_INFO (
	VEHICLE_ID STRING,
	MODEL STRING,
	MAKE_YEAR INT,
	COLOR STRING
)
 EXTERNAL_VOLUME = 'AWS_S3_EXT_VOLUME_SNOWFLAKE'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'raw/vehicle_info/'
 CATALOG_SYNC = 'polaris_external_cat_integration';

create or replace ICEBERG TABLE DASH_DB.RAW.STREAMING_VEHICLE_EVENTS (
	VEHICLE_ID STRING,
	EVENT_CREATED_AT TIMESTAMP_NTZ(6),
	LATITUDE FLOAT,
	LONGITUDE FLOAT,
	SPEED FLOAT,
	ENGINE_STATUS STRING,
	FUEL_CONSUMPTION_CURRENT FLOAT,
	FUEL_CONSUMPTION_AVERAGE FLOAT,
	FUEL_CONSUMPTION_UNIT STRING,
	HARD_ACCELERATIONS INT,
	SMOOTH_ACCELERATIONS INT,
	HARD_BRAKES INT,
	SMOOTH_BRAKES INT,
	SHARP_TURNS INT,
	GENTLE_TURNS INT,
	MAINTENANCE_REQUIRED BOOLEAN
)
 EXTERNAL_VOLUME = 'AWS_S3_EXT_VOLUME_SNOWFLAKE'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'raw/streaming_vehicle_events/'
 CATALOG_SYNC = 'polaris_external_cat_integration';

create or replace dynamic iceberg table DASH_DB.REFINED.VEHICLE_EVENTS_SCD2(
	VEHICLE_ID,
	EVENT_START_DATE,
	EVENT_END_DATE,
	ENGINE_STATUS
) target_lag = '1 minute' refresh_mode = AUTO initialize = ON_CREATE warehouse = DASH_L
 EXTERNAL_VOLUME = 'aws_s3_ext_volume_snowflake'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'refined/vehicle_events_scd2/'
 CATALOG_SYNC = 'polaris_external_cat_integration'
 as
    SELECT
        vehicle_id,
        event_created_at::DATE AS event_start_date,
        TO_DATE(LEAD(event_created_at) OVER (PARTITION BY vehicle_id ORDER BY event_created_at ASC)) AS event_end_date,
        engine_status
    FROM dash_db.raw.streaming_vehicle_events
    ORDER BY
        vehicle_id,
        event_start_date ASC,
        event_end_date DESC
;

create or replace dynamic iceberg table DASH_DB.REFINED.VEHICLE_MODELS_EVENTS(
	VEHICLE_ID,
	EVENT_START_DATE,
	EVENT_END_DATE,
	ENGINE_STATUS,
	MODEL,
	MAKE_YEAR,
	COLOR
) target_lag = '1 minute' refresh_mode = AUTO initialize = ON_CREATE warehouse = DASH_L
 EXTERNAL_VOLUME = 'aws_s3_ext_volume_snowflake'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'refined/vehicle_models_events/'
 CATALOG_SYNC = 'polaris_external_cat_integration'
 as
    SELECT
        e.*,
        i.model,
        i.make_year,
        i.color
    FROM dash_db.refined.vehicle_events_scd2 e
    LEFT JOIN dash_db.raw.vehicle_info i
        ON e.vehicle_id = i.vehicle_id
    ORDER BY
        vehicle_id,
        event_start_date ASC,
        event_end_date DESC
;
