/*
{{
    config(
        materialized='incremental',
        unique_key='P_ID',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'       
    )
}}

with final as (
    select * from {{ref('testt1')}}
    {% if is_incremental() %}
    where P_ID >= (select max(P_ID) from {{ this }})
    {% endif %}
)

select * from final

*/
