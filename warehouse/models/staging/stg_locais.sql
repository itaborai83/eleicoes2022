{{ 
	config(
		materialized='table'
	,	unique_key="id"
	,	indexes=[ 
			{'columns': ['id']}
		,	{'columns': ['sg_uf', 'cd_municipio', 'nr_zona', 'nr_secao']}
		]
	)
}}

WITH source_data AS (
	SELECT	DISTINCT
			sg_uf || '-' || cd_municipio || '-' || nr_zona || '-' || nr_secao AS id
	,		sg_uf
	,		cd_municipio
	,		nm_municipio
	,		nr_zona
	,		nr_secao
	,		nr_local_votacao
	FROM	stg_boletim
)
SELECT 	*
FROM 	source_data



