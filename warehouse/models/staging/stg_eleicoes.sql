{{ 
	config(
		materialized='table'
	,	unique_key="id"
	,	indexes=[ {'columns': ['id']} ]
	) 
}}

WITH source_data AS (
	SELECT	DISTINCT
			cd_eleicao AS id
	,		ano_eleicao
	,		nr_turno
	,		cd_tipo_eleicao
	,		nm_tipo_eleicao
	,		cd_eleicao
	,		ds_eleicao
	,		cd_pleito
	,		dt_pleito
	FROM	stg_boletim
)
SELECT *
FROM source_data