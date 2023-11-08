{% macro anonymize_columns(database, schema, table) %}
   {% call statement('mapping', fetch_result=True) %}
       select *
       from {{ ref('anonymization_mapping') }} as am
       where am.table_name = '{{ table }}'
           and am.schema_name = '{{ schema }}'
       order by am.column_name;
   {% endcall %}

   {% call statement('surrogate_keys', fetch_result=True) %}
		select include_columns 
		from {{ ref('surrogate_key_mapping') }}
    	where table_name = '{{ table }}'
    	    and schema_name = '{{ schema }}'
		limit 1;
   {% endcall %}

   {% set mapping = load_result('mapping')['data'] %}
   {% set surrogate_keys =  load_result('surrogate_keys')['data'] %}
   -- map[0]: schema_name,
   -- map[1]: table_name,
   -- map[2]: column_name,
   -- map[3]: target_function, function to anonymise the field
   -- map[4]: target_type, type to convert to
   {% for map in mapping %}
       {% if map[0] == schema and  map[1] == table and map[3] != none  %}
       		{% if map[3] == 'surrogate_key' %}
           		{{ anonymize(map[2],map[3],map[4],surrogate_keys=surrogate_keys[0]) }}
       		{% else %}
           		{{ anonymize(map[2],map[3],map[4]) }}
       		{% endif %}
       {% else %}
		   {{ normalize(map[2],map[4]) }} as {{ map[2] }}
       {% endif %}
       {% if not loop.last %}
       		,
       {% endif %}
   {% endfor %}
   
{% endmacro %}