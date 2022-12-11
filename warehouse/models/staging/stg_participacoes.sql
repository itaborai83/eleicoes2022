{{ 
	config(
		materialized='table'
	,	unique_key="id"
	,	indexes=[ 
			{'columns': ['id']} 
		,	{'columns': ['sg_uf', 'cd_municipio', 'nr_zona', 'nr_secao', 'cd_eleicao']}
		]
	) 
}}

WITH source_data AS (
	SELECT	DISTINCT
			sg_uf || '-' || cd_municipio || '-' || nr_zona || '-' || nr_secao || '-' || cd_eleicao AS id
	,		sg_uf
	,		cd_municipio
	,		nr_zona
	,		nr_secao
	,		cd_eleicao
	,		qt_aptos
	,		qt_comparecimento
	,		qt_abstencoes
	FROM	stg_boletim
)
SELECT *
FROM source_data