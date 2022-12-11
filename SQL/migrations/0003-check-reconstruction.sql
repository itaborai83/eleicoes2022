SELECT	a.ano_eleicao
,		a.nr_turno 
,		a.cd_tipo_eleicao 
,		a.nm_tipo_eleicao
,		a.cd_eleicao
,		a.ds_eleicao
,		a.cd_pleito
,		a.dt_pleito
,		b.sg_uf
,		b.cd_municipio 
,		b.nm_municipio 
,		b.nr_zona
,		b.nr_secao 
,		b.nr_local_votacao 
,		c.qt_aptos
,		c.qt_comparecimento 
,		c.qt_abstencoes 
,		d.nr_partido
,		d.sg_partido
,		d.nm_partido
,		d.cd_tipo_votavel 
,		d.ds_tipo_votavel
,		d.nr_votavel 
,		d.nm_votavel 
,		d.cd_cargo_pergunta
,		e.cd_tipo_urna 
,		e.ds_tipo_urna 
,		e.nr_urna_efetivada 
,		e.cd_pleito 
,		e.cd_carga_1_urna_efetivada 
,		e.cd_carga_2_urna_efetivada 
,		e.cd_flashcard_urna_efetivada 
,		e.dt_carga_urna_efetivada 
,		e.ds_agregadas 
,		e.dt_abertura 
,		e.dt_encerramento 
,		e.qt_eleitores_biometria_nh 
,		e.dt_emissao_bu 
,		e.nr_junta_apuradora 
,		e.nr_turma_apuradora 
FROM	stg_eleicao a
,		stg_local b
,		stg_participacao c
,		stg_votavel d
,		stg_urna e
,		stg_voto f
WHERE	b.nm_municipio 		= 'VITÓRIA'
AND		f.eleicao_id		= a.id
AND		f.local_id			= b.id
AND		f.participacao_id	= c.id
AND		f.votavel_id		= d.id
AND		f.urna_id			= e.id

--
EXCEPT
--

SELECT	a.ano_eleicao
,		a.nr_turno 
,		a.cd_tipo_eleicao 
,		a.nm_tipo_eleicao
,		a.cd_eleicao
,		a.ds_eleicao
,		a.cd_pleito
,		a.dt_pleito
,		a.sg_uf
,		a.cd_municipio 
,		a.nm_municipio 
,		a.nr_zona
,		a.nr_secao 
,		a.nr_local_votacao 
,		a.qt_aptos
,		a.qt_comparecimento 
,		a.qt_abstencoes 
,		COALESCE(a.nr_partido, -99) AS nr_partido
,		COALESCE(a.sg_partido, '<N/A>') AS sg_partido
,		COALESCE(a.nm_partido, '<N/A>') AS nm_partido
,		a.cd_tipo_votavel 
,		a.ds_tipo_votavel
,		a.nr_votavel 
,		a.nm_votavel 
,		a.cd_cargo_pergunta
,		a.cd_tipo_urna 
,		a.ds_tipo_urna 
,		a.nr_urna_efetivada 
,		a.cd_pleito 
,		a.cd_carga_1_urna_efetivada 
,		a.cd_carga_2_urna_efetivada 
,		a.cd_flashcard_urna_efetivada 
,		a.dt_carga_urna_efetivada 
,		a.ds_agregadas 
,		a.dt_abertura 
,		a.dt_encerramento 
,		a.qt_eleitores_biometria_nh 
,		a.dt_emissao_bu 
,		COALESCE(a.nr_junta_apuradora, -9999) AS nr_junta_apuradora 
,		COALESCE(a.nr_turma_apuradora, -9999) AS nr_turma_apuradora
FROM	stg_boletim a
WHERE	a.nm_municipio = 'VITÓRIA'w