{{ 
	config(
		materialized='table'
	,	unique_key="id"
	,	indexes=[ 
			{'columns': ['id']} 
		,	{'columns': ['cd_pleito', 'sg_uf', 'nr_urna_efetivada', 'nr_junta_apuradora', 'nr_turma_apuradora']}
		]
	) 
}}

WITH source_data AS (
	SELECT	DISTINCT
			cd_pleito || '-' || sg_uf || '-' || nr_urna_efetivada || '-' || COALESCE(nr_junta_apuradora, 9999) || '-' || COALESCE(nr_turma_apuradora, 9999) AS id
	,		sg_uf
	,		cd_tipo_urna
	,		ds_tipo_urna
	,		nr_urna_efetivada
	,		cd_pleito
	,		cd_carga_1_urna_efetivada
	,		cd_carga_2_urna_efetivada
	,		cd_flashcard_urna_efetivada
	,		dt_carga_urna_efetivada
	,		ds_agregadas
	,		dt_abertura
	,		dt_encerramento
	,		qt_eleitores_biometria_nh
	,		dt_emissao_bu
	,		COALESCE(nr_junta_apuradora, 9999) AS nr_junta_apuradora
	,		COALESCE(nr_turma_apuradora, 9999) AS nr_turma_apuradora
	FROM	stg_boletim
)
SELECT *
FROM source_data