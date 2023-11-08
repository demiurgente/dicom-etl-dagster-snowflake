 -- depends_on: {{ ref('staged_patients') }}

{{
	config(
		materialized='incremental',
		unique_key=[
			'other_patient_ids',
		    'patient_age',
			'patient_birth_date',
			'patient_id',
			'patient_name',
		]
	)
}}

SELECT *
FROM {{ ref('staged_patients') }} sp
