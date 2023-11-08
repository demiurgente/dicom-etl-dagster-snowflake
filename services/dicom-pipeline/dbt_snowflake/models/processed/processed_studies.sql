 -- depends_on: {{ ref('staged_studies') }}

{{
	config(
		materialized='incremental',
    	unique_key='study_instance_uid',
    	partition_by={
    	    "field": "study_date",
    	    "data_type": "timestamp",
    	    "granularity": "month"
    	}
	)
}}  

SELECT *
FROM {{ ref('staged_studies') }} ss

-- only on an incremental run
{% if is_incremental() %}
	WHERE ss.study_date > '{{ get_max_insert_date(study_date) }}'
{% endif %}
----

