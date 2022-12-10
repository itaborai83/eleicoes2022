---------------------------------------------------------------------------------------------------
-- Eleição
---------------------------------------------------------------------------------------------------
DELETE FROM stg_eleicao;
INSERT INTO stg_eleicao(
		ano_eleicao					-- INTEGER
,		nr_turno					-- INTEGER
,		cd_tipo_eleicao             -- INTEGER
,		nm_tipo_eleicao             -- TEXT
,		cd_eleicao                  -- INTEGER
,		ds_eleicao                  -- TEXT
,		cd_pleito                   -- INTEGER
,		dt_pleito                   -- TEXT
)
SELECT	DISTINCT
		ano_eleicao
,		nr_turno
,		cd_tipo_eleicao
,		nm_tipo_eleicao
,		cd_eleicao
,		ds_eleicao
,		cd_pleito
,		dt_pleito
FROM	stg_boletim;

---------------------------------------------------------------------------------------------------
-- Local
---------------------------------------------------------------------------------------------------
DELETE FROM stg_local;
INSERT INTO stg_local(
		sg_uf						-- TEXT
,		cd_municipio                -- INTEGER
,		nm_municipio                -- TEXT
,		nr_zona                     -- INTEGER
,		nr_secao                    -- INTEGER
,		nr_local_votacao            -- INTEGER
)
SELECT	DISTINCT
		sg_uf
,		cd_municipio
,		nm_municipio
,		nr_zona
,		nr_secao
,		nr_local_votacao
FROM	stg_boletim;

---------------------------------------------------------------------------------------------------
-- Participação
---------------------------------------------------------------------------------------------------
DELETE FROM stg_participacao;
INSERT INTO stg_participacao(		
		sg_uf                       -- TEXT
,		cd_municipio                -- INTEGER
,		nr_zona                     -- INTEGER
,		nr_secao                    -- INTEGER
,		cd_eleicao                  -- INTEGER
,		qt_aptos                    -- INTEGER
,		qt_comparecimento           -- INTEGER
,		qt_abstencoes               -- INTEGER
)
SELECT	DISTINCT
		sg_uf
,		cd_municipio
,		nr_zona
,		nr_secao
,		cd_eleicao
,		qt_aptos
,		qt_comparecimento
,		qt_abstencoes
FROM	stg_boletim;

---------------------------------------------------------------------------------------------------
-- Votável
---------------------------------------------------------------------------------------------------
DELETE FROM stg_votavel;
INSERT INTO stg_votavel(
		nr_partido					-- INTEGER
,		sg_partido                  -- TEXT
,		nm_partido                  -- TEXT
,		cd_tipo_votavel             -- INTEGER
,		ds_tipo_votavel             -- TEXT
,		nr_votavel                  -- INTEGER
,		nm_votavel                  -- TEXT
,		cd_cargo_pergunta           -- INTEGER
,		ds_cargo_pergunta           -- TEXT
,		sg_uf_cargo           		-- TEXT
)
SELECT	DISTINCT
		COALESCE(nr_partido, -99) AS nr_partido
,		COALESCE(sg_partido, '<N/A>') AS sg_partido
,		COALESCE(nm_partido, '<N/A>') AS nm_partido
,		cd_tipo_votavel
,		ds_tipo_votavel
,		nr_votavel
,		nm_votavel
,		cd_cargo_pergunta 
,		ds_cargo_pergunta 
,		sg_uf sg_uf_cargo
FROM	stg_boletim;

---------------------------------------------------------------------------------------------------
-- Urna
---------------------------------------------------------------------------------------------------
DELETE FROM stg_urna;
INSERT INTO stg_urna(
		cd_tipo_urna				-- INTEGER
,		ds_tipo_urna                -- TEXT
,		nr_urna_efetivada           -- INTEGER
,		cd_pleito					-- INTEGER
,		cd_carga_1_urna_efetivada   -- TEXT
,		cd_carga_2_urna_efetivada   -- TEXT
,		cd_flashcard_urna_efetivada -- TEXT
,		dt_carga_urna_efetivada     -- TEXT
,		ds_agregadas                -- TEXT
,		dt_abertura                 -- TEXT
,		dt_encerramento             -- TEXT
,		qt_eleitores_biometria_nh   -- INTEGER
,		dt_emissao_bu               -- TEXT
,		nr_junta_apuradora          -- INTEGER
,		nr_turma_apuradora          -- INTEGER
)
SELECT	DISTINCT
		cd_tipo_urna
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
,		COALESCE(nr_junta_apuradora, -9999) 
,		COALESCE(nr_turma_apuradora, -9999)
FROM	stg_boletim;

---------------------------------------------------------------------------------------------------
-- Voto
---------------------------------------------------------------------------------------------------
DELETE FROM stg_voto;
INSERT INTO stg_voto(
		eleicao_id					-- INTEGER
,		local_id					-- INTEGER
,		participacao_id				-- INTEGER
,		votavel_id					-- INTEGER
,		urna_id						-- INTEGER
,		qt_votos					-- INTEGER
)SELECT	(
			SELECT	id
			FROM 	stg_eleicao 
			WHERE cd_eleicao = a.cd_eleicao
		) AS eleicao_id
		--
,		(
			SELECT 	id 
			FROM 	stg_local 
			WHERE 	sg_uf = a.sg_uf 
			AND 	cd_municipio = a.cd_municipio 
			AND 	nr_zona = a.nr_zona 
			AND 	nr_secao = a.nr_secao
		) AS local_id
		--
,		(	
			SELECT 	id 
			FROM 	stg_participacao 
			WHERE 	sg_uf 				= a.sg_uf 
			AND 	cd_municipio 		= a.cd_municipio 
			AND 	nr_zona 			= a.nr_zona 
			AND 	nr_secao 			= a.nr_secao 
			AND 	cd_eleicao 			= a.cd_eleicao
		) AS participacao_id
		--
,		(
			SELECT 	id 
			FROM 	stg_votavel 
			WHERE 	nr_partido 			= COALESCE(a.nr_partido, -99)
			AND		cd_tipo_votavel		= a.cd_tipo_votavel 
			AND		nr_votavel			= a.nr_votavel 
			AND		cd_cargo_pergunta	= a.cd_cargo_pergunta 
			AND		sg_uf_cargo			= a.sg_uf 
		) AS votavel_id
		--
,		(
			SELECT	id
			FROM	stg_urna
			WHERE	nr_urna_efetivada	= a.nr_urna_efetivada
			AND		cd_pleito			= a.cd_pleito
			AND		nr_junta_apuradora	= COALESCE(nr_junta_apuradora, -9999)
			AND		nr_turma_apuradora	= COALESCE(nr_turma_apuradora, -9999)
		) AS urna_id
,		qt_votos
FROM	stg_boletim a;
