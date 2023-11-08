 -- depends_on: {{ ref('staged_devices') }}

{{
	config(
		materialized='incremental',
		unique_key='device_serial_number'
	)
}}

SELECT *
FROM {{ ref('staged_devices') }} sd
