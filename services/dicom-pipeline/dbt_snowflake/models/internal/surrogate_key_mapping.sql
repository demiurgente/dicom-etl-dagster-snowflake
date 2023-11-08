 -- depends_on: {{ ref('salt') }}
 -- depends_on: {{ ref('anonymization_mapping') }}
WITH COLUMN_LIST AS (
	select
		schema_name,
		table_name,
		GROUP_CONCAT(column_name) AS include_columns
	from {{ ref('anonymization_mapping') }}
	where target_function = 'surrogate_key'
	group by schema_name, table_name
)

select
  cl.schema_name,
  cl.table_name,
  column_name,
  cl.include_columns
from {{ ref('anonymization_mapping') }} am
inner join COLUMN_LIST as cl
	on am.table_name = cl.table_name
	and am.schema_name = cl.schema_name
	and target_function = 'surrogate_key'
order by column_name