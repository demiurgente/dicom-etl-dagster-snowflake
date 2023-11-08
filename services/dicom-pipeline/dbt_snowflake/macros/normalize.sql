{%  macro normalize(column_name, target_type) %}
   {% if execute %}
    	coalesce(try_cast(lower(trim({{ column_name }}, '*')) as {{ target_type }}))
   {% endif %}
{% endmacro %}