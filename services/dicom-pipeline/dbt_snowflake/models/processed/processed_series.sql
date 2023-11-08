 -- depends_on: {{ ref('staged_series') }}

{{ 
	config(
		materialized='incremental',
		unique_key='series_instance_uid',
		partition_by={
            "field": "study_date",
            "data_type": "timestamp",
            "granularity": "month"
        }
	)
}}

SELECT *
FROM {{ ref('staged_series') }} ss

-- only on an incremental run
{% if is_incremental() %}
	WHERE ss.study_date > '{{ get_max_insert_date(study_date) }}'
{% endif %}
----
