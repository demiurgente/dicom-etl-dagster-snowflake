{% macro get_max_insert_date(date_column) %}
	{% if execute and 
	      is_incremental() and 
	      env_var('ENABLE_MAX_INSERT_DATE_MACRO', '1') == '1' %}
		{% set query %}
			SELECT max(date_column) FROM {{ this }};
		{% endset %}

		{% set max_event_time = run_query(query).columns[0][0] %}

		{% do return(max_event_time) %}
	{% endif %}
{% endmacro %}