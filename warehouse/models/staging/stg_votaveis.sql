{{ 
	config(
		materialized='table'
	,	unique_key="id"
	,	indexes=[ 
			{'columns': ['id']} 
		,	{'columns': ['sg_uf_cargo', 'cd_cargo_pergunta', 'cd_tipo_votavel', 'nr_partido', 'cd_tipo_votavel', 'nr_votavel']}
		]
	) 
}}

WITH source_data AS (
	SELECT	DISTINCT
			sg_uf || '-' || cd_cargo_pergunta || '-' || cd_tipo_votavel || '-' || COALESCE(nr_partido, 99) || '-' || nr_votavel AS id
	,		COALESCE(nr_partido, 99) AS nr_partido
	,		COALESCE(sg_partido, '<N/A>') AS sg_partido
	,		COALESCE(nm_partido, '<N/A>') AS nm_partido
	,		cd_tipo_votavel
	,		ds_tipo_votavel
	,		nr_votavel
	,		nm_votavel
	,		cd_cargo_pergunta 
	,		ds_cargo_pergunta 
	,		sg_uf sg_uf_cargo
	FROM	stg_boletim
)
SELECT *
FROM source_data