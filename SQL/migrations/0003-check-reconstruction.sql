SELECT	DISTINCT
		ano_eleicao
,		cd_tipo_eleicao
,		nm_tipo_eleicao
,		cd_pleito
,		dt_pleito
,		nr_turno
,		cd_eleicao
,		ds_eleicao
,		sg_uf
,		cd_municipio
,		nm_municipio
,		nr_zona
,		nr_secao
,		nr_local_votacao
/*
,		cd_cargo_pergunta
,		ds_cargo_pergunta
,		nr_partido
,		sg_partido
,		nm_partido
,		dt_bu_recebido
,		qt_aptos
,		qt_comparecimento
,		qt_abstencoes
,		cd_tipo_urna
,		ds_tipo_urna
,		cd_tipo_votavel
,		ds_tipo_votavel
,		nr_votavel
,		nm_votavel
,		qt_votos
,		nr_urna_efetivada
,		cd_carga_1_urna_efetivada
,		cd_carga_2_urna_efetivada
,		cd_flashcard_urna_efetivada
,		dt_carga_urna_efetivada
,		ds_cargo_pergunta_secao
,		ds_agregadas
,		dt_abertura
,		dt_encerramento
,		qt_eleitores_biometria_nh
,		dt_emissao_bu
*/
FROM	stg_boletim

--
UNION
--

SELECT	DISTINCT 
		b.ano_eleicao
,		c.cd_tipo_eleicao
,		c.nm_tipo_eleicao
,		a.cd_pleito
,		a.dt_pleito
,		b.nr_turno
,		b.cd_eleicao
,		b.ds_eleicao
,		f.sg_uf
,		f.cd_municipio
,		f.nm_municipio
,		e.nr_zona
,		e.nr_secao
,		e.nr_local_votacao
FROM	stg_pleito a
		INNER JOIN stg_eleicao b ON a.cd_pleito = b.cd_pleito 
		INNER JOIN stg_tipo_eleicao c ON b.cd_tipo_eleicao = c.cd_tipo_eleicao 
		INNER JOIN stg_participacao d ON b.cd_eleicao = d.cd_eleicao 
		INNER JOIN stg_zona_secao e ON d.nr_zona = e.nr_zona AND d.nr_secao = e.nr_secao AND d.cd_municipio = e.cd_municipio
		INNER JOIN stg_municipio f ON e.cd_municipio = f.cd_municipio 
		INNER JOIN stg_local_votacao g ON e.nr_local_votacao = g.nr_local_votacao
		INNER JOIN stg_votavel h ON b.cd_eleicao = h.cd_eleicao 
		INNER JOIN stg_tipo_votavel i ON h.cd_tipo_votavel = i.cd_tipo_votavel 
		INNER JOIN stg_cargo_pergunta j ON h.cd_cargo_pergunta = j.cd_cargo_pergunta 
		INNER JOIN stg_partido k ON h.nr_partido = k.nr_partido 
		
		select * from stg_votavel