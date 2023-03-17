{{ config(materialized='view') }}


select
P_ID,
TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP())) AS TIMESTAMP,
COLUMN1,
PARSE_JSON(COLUMN1) as JSON_OBJECT
FROM {{source('snowflake_json_data1','TEST_INSERT')}}