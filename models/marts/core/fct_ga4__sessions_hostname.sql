-- Fact table for sessions. Join on session_key and hostname

with session_metrics as 
(
    select 
        session_key,
        client_id,
        page_hostname,
        min(event_date_dt) as session_start_date,
        min(event_timestamp) as session_start_timestamp,
        countif(event_name = 'page_view') as count_page_views,
        sum(event_value_in_usd) as sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_engaged,
        sum(engagement_time_msec) as sum_engagement_time_msec
    from {{ref('stg_ga4__events')}}
    group by 1,2,3
)
{% if var('conversion_events',false) %}
,
join_conversions as (
    select
        *
    from session_metrics
    left join {{ref('stg_ga4__session_hostname_conversions')}} using (session_key, page_hostname)
)
select * from join_conversions
{% else %}
select * from session_metrics
{% endif %}

