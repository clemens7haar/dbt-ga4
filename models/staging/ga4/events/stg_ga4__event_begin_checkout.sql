{% if var('ecommerce', false ) ==  false %}
  {{
    config(
        enabled = false,
    )
  }}
{% endif %}
 with begin_checkout_with_params as (
   select *
    {% if var("begin_checkout_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("begin_checkout_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'begin_checkout'
)

select * from begin_checkout_with_params