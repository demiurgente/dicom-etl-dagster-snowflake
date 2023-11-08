 -- depends_on: {{ ref('salt') }}
 -- depends_on: {{ ref('anonymization_mapping') }}
 -- depends_on: {{ ref('surrogate_key_mapping') }}

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

SELECT {{ anonymize_columns('dicom', 'raw', 'patients') }}
FROM {{ source('raw','patients') }} rp
