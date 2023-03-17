{{
    config(
        materialized='incremental',
        unique_key='P_ID',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'       
    )
}}

{% set flattened_query %}
SELECT distinct json1.key as column_names
FROM {{ref('sql1')}},
LATERAL FLATTEN (input => JSON_OBJECT) AS json1
{% endset %}

{% set results = run_query(flattened_query) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for column in results_list %}
    {% do log("The column name is: " ~ column ) %}
{% endfor %}

{% set mapping_query %}
    SELECT existing_c, new_c
    FROM {{source('snowflake_json_data1','MAPPING')}}
{% endset %}

{% set mapping_data = run_query(mapping_query) %}

{% for row in mapping_data %}
    {% set exsisting_column = row.0 %}
    {% set new_column = row.1 %}
    {% do log("The mapping dictionary is: {'exsisting_column': '" ~ exsisting_column ~ "', 'new_column': '" ~ new_column ~ "'}") %}
{% endfor %}



{% set column_mapping = {} %}
{% for row in mapping_data %}
    {% set existing_column = row.0 %}
    {% set new_column = row.1 %}
    {% set _ = column_mapping.update({existing_column: new_column}) %}
    {% do log("The column mapping is: " ~ column_mapping ) %}
{% endfor %}


with final as(
SELECT
P_ID,
TIMESTAMP_UTC,
JSON_OBJECT,
{% for column in results_list %}
    {% if column in column_mapping %}
        JSON_OBJECT:{{ column}}::varchar AS {{ column_mapping[column]}}{% if not loop.last %},{% endif %}    
    {% else %}    
        JSON_OBJECT:{{ column }}::varchar AS {{ column|replace(".", "_")|replace(":", "_")|replace("-", "_") }}{% if not loop.last %},{% endif %}
    {% endif %}
{% endfor %}
FROM {{ ref('sql1')}}
)

select * from final
{% if is_incremental() %}
where P_ID >= (select max(P_ID) from {{ this }})
{% endif %}






