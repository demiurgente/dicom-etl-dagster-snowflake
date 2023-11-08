{%  macro anonymize(column_name, target_function, target_type, add_alias=True, surrogate_keys=none) %}
   {% if execute %}
       {% call statement('salt', fetch_result=True) %}
			select salt from {{ ref('salt') }} limit 1;
       {% endcall %}
       {% set salt = load_result('salt')['data'][0][0] %}
	   {% set normalized_column = normalize(column_name, target_type) %} 

       {% if target_function == 'anon_text' %}
		   'ANON_TEXT'
       {% endif %}

       {% if target_function == 'full_hash' %}
           SHA256(concat({{ normalized_column }},'{{ salt }}'))
       {% endif %}

       {% if target_function == 'surrogate_key' %}
	   	   {% set columns = surrogate_keys[0].split(",") %}
           SHA256(concat({{ dbt_utils.generate_surrogate_key(columns) }},'{{ salt }}'))
       {% endif %}
       {% if target_function == none %}
	   	   {{ normalized_column }}
       {% endif %}

       {% if add_alias %}
           as {{ column_name }}
       {% endif %}
   {% endif %}
{% endmacro %}