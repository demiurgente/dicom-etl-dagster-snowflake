 -- depends_on: {{ ref('salt') }}
 -- depends_on: {{ ref('anonymization_mapping') }}
 -- depends_on: {{ ref('surrogate_key_mapping') }}

{{ 
	config(
		materialized='incremental',
		unique_key='device_serial_number'
	)
}}

SELECT {{ anonymize_columns('dicom', 'raw', 'devices') }}
FROM {{ source('raw','devices') }} rd