{{ 
	config(
		materialized='table'
	,	unique_key="id"
	,	indexes=[ 
			{'columns': ['eleicao_id']} 
		,	{'columns': ['local_id']}
		,	{'columns': ['participacao_id']}
		,	{'columns': ['votavel_id']}
		,	{'columns': ['urna_id']}
		]
	)
}}

WITH source_data AS (
	SELECT	cd_eleicao AS eleicao_id
			--
	,		sg_uf || '-' || cd_municipio || '-' || nr_zona || '-' || nr_secao AS local_id
			--
	,		sg_uf || '-' || cd_municipio || '-' || nr_zona || '-' || nr_secao || '-' || cd_eleicao || '-' || cd_pleito AS participacao_id
			--
	,		sg_uf || '-' || cd_cargo_pergunta || '-' || cd_tipo_votavel || '-' || COALESCE(nr_partido, 99) || '-' || nr_votavel AS votavel_id
			--
	,		cd_pleito || '-' || sg_uf || '-' || nr_urna_efetivada || '-' || COALESCE(nr_junta_apuradora, 9999) || '-' || COALESCE(nr_turma_apuradora, 9999) AS urna_id
			--
	,		qt_votos
	FROM	stg_boletim a
)
SELECT *
FROM source_data