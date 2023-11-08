 -- depends_on: {{ ref('salt') }}
 -- depends_on: {{ ref('anonymization_mapping') }}
 -- depends_on: {{ ref('surrogate_key_mapping') }}

{{
	config(
		materialized='incremental',
		unique_key=[
			'series_instance_uid',
			'sop_instance_uid'
		],
		incremental_strategy = 'insert_overwrite',
		partition_by={
            "field": "instance_creation_date",
            "data_type": "timestamp",
            "granularity": "month"
        }
	)
}}

SELECT {{ anonymize_columns('dicom', 'raw', 'images') }}
FROM {{ source('raw','images') }} ri

-- only on an incremental run
{% if is_incremental() %}
	WHERE ri.instance_created_date > '{{ get_max_insert_date(instance_created_date) }}'
{% endif %}
----
