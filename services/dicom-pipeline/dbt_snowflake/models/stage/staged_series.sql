 -- depends_on: {{ ref('salt') }}
 -- depends_on: {{ ref('anonymization_mapping') }}
 -- depends_on: {{ ref('surrogate_key_mapping') }}

{{ 
	config(
		materialized='incremental',
		unique_key='series_instance_uid',
		incremental_strategy = 'insert_overwrite',
		partition_by={
            "field": "study_date",
            "data_type": "timestamp",
            "granularity": "month"
        }
	)
}}

SELECT {{ anonymize_columns('dicom', 'raw', 'series') }}
FROM {{ source('raw','series') }} rs

-- only on an incremental run
{% if is_incremental() %}
	WHERE rs.study_date > '{{ get_max_insert_date(study_date) }}'
{% endif %}
----
