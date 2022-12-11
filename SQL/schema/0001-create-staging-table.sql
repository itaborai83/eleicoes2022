---------------------------------------------------------------------------------------------------
-- Boletim
---------------------------------------------------------------------------------------------------

-- DROP TABLE stg_boletim
CREATE TABLE IF NOT EXISTS stg_boletim(
	dt_geracao						TEXT    -- 05/10/2022
,	hh_geracao                      TEXT    -- 15:26:45
,	ano_eleicao                     INTEGER -- 2022
,	cd_tipo_eleicao                 INTEGER -- 0
,	nm_tipo_eleicao                 TEXT    -- Eleição Ordinária
,	cd_pleito                       INTEGER -- 406
,	dt_pleito                       TEXT    -- 02/10/2022
,	nr_turno                        INTEGER -- 1
,	cd_eleicao                      INTEGER -- 544
,	ds_eleicao                      TEXT    -- Eleição Geral Federal 2022
,	sg_uf                           TEXT    -- RS
,	cd_municipio                    INTEGER -- 88013
,	nm_municipio                    TEXT    -- PORTO ALEGRE
,	nr_zona                         INTEGER -- 1
,	nr_secao                        INTEGER -- 1
,	nr_local_votacao                INTEGER -- 1422
,	cd_cargo_pergunta               INTEGER -- 1
,	ds_cargo_pergunta               TEXT    -- Presidente
,	nr_partido                      INTEGER -- -1
,	sg_partido                      TEXT    -- #NULO#
,	nm_partido                      TEXT    -- #NULO#
,	dt_bu_recebido                  TEXT    -- 02/10/2022 18:55:58
,	qt_aptos                        INTEGER -- 354
,	qt_comparecimento               INTEGER -- 280
,	qt_abstencoes                   INTEGER -- 74
,	cd_tipo_urna                    INTEGER -- 1
,	ds_tipo_urna                    TEXT    -- APURADA
,	cd_tipo_votavel                 INTEGER -- 2
,	ds_tipo_votavel                 TEXT    -- Branco
,	nr_votavel                      INTEGER -- 95
,	nm_votavel                      TEXT    -- Branco
,	qt_votos                        INTEGER -- 3
,	nr_urna_efetivada               INTEGER -- 2215453
,	cd_carga_1_urna_efetivada       TEXT    -- 787.689.897.600.495.174.
,	cd_carga_2_urna_efetivada       TEXT    -- 531.998
,	cd_flashcard_urna_efetivada     TEXT    -- A9CA01B1
,	dt_carga_urna_efetivada         TEXT    -- 22/09/2022 14:12:00
,	ds_cargo_pergunta_secao         TEXT    -- 1 - 1
,	ds_agregadas                    TEXT    -- #NULO#
,	dt_abertura                     TEXT    -- 02/10/2022 08:00:01
,	dt_encerramento                 TEXT    -- 02/10/2022 17:01:25
,	qt_eleitores_biometria_nh       INTEGER -- 15
,	dt_emissao_bu                   TEXT    -- 02/10/2022 17:04:05
,	nr_junta_apuradora				INTEGER
,	nr_turma_apuradora				INTEGER
);
/*
---------------------------------------------------------------------------------------------------
-- Eleição
---------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg_eleicao;
CREATE TABLE stg_eleicao(
		id							SERIAL PRIMARY KEY
,		ano_eleicao					INTEGER
,		nr_turno					INTEGER
,		cd_tipo_eleicao             INTEGER
,		nm_tipo_eleicao             TEXT
,		cd_eleicao                  INTEGER
,		ds_eleicao                  TEXT
,		cd_pleito                   INTEGER
,		dt_pleito                   TEXT
,		UNIQUE(cd_eleicao)
);

---------------------------------------------------------------------------------------------------
-- Local
---------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg_local;
CREATE TABLE stg_local(
		id							SERIAL PRIMARY KEY
,		sg_uf						TEXT
,		cd_municipio                INTEGER
,		nm_municipio                TEXT
,		nr_zona                     INTEGER
,		nr_secao                    INTEGER
,		nr_local_votacao            INTEGER
,		UNIQUE(sg_uf, cd_municipio, nr_zona, nr_secao)
);


---------------------------------------------------------------------------------------------------
-- Participação
---------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg_participacao;
CREATE TABLE stg_participacao(		
		id							SERIAL PRIMARY KEY
,		sg_uf                       TEXT
,		cd_municipio                INTEGER
,		nr_zona                     INTEGER
,		nr_secao                    INTEGER
,		cd_eleicao                  INTEGER
,		qt_aptos                    INTEGER
,		qt_comparecimento           INTEGER
,		qt_abstencoes               INTEGER
,		UNIQUE(sg_uf, cd_municipio, nr_zona, nr_secao, cd_eleicao)
);


---------------------------------------------------------------------------------------------------
-- Votável
---------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg_votavel;
CREATE TABLE stg_votavel(
		id							SERIAL PRIMARY KEY
,		nr_partido					INTEGER
,		sg_partido                  TEXT
,		nm_partido                  TEXT
,		cd_tipo_votavel             INTEGER
,		ds_tipo_votavel             TEXT
,		nr_votavel                  INTEGER
,		nm_votavel                  TEXT
,		cd_cargo_pergunta           INTEGER
,		ds_cargo_pergunta           TEXT
,		sg_uf_cargo           		TEXT
,		UNIQUE(nr_partido, cd_tipo_votavel, nr_votavel, cd_cargo_pergunta, sg_uf_cargo)
);

---------------------------------------------------------------------------------------------------
-- Urna
---------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg_urna;
CREATE TABLE stg_urna(
		id							SERIAL PRIMARY KEY
,		cd_tipo_urna				INTEGER
,		ds_tipo_urna                TEXT
,		nr_urna_efetivada           INTEGER
,		cd_pleito					INTEGER
,		cd_carga_1_urna_efetivada   TEXT
,		cd_carga_2_urna_efetivada   TEXT
,		cd_flashcard_urna_efetivada TEXT
,		dt_carga_urna_efetivada     TEXT
,		ds_agregadas                TEXT
,		dt_abertura                 TEXT
,		dt_encerramento             TEXT
,		qt_eleitores_biometria_nh   INTEGER
,		dt_emissao_bu               TEXT
,		nr_junta_apuradora          INTEGER
,		nr_turma_apuradora          INTEGER
,		UNIQUE(nr_urna_efetivada, cd_pleito, nr_junta_apuradora, nr_turma_apuradora)
);

---------------------------------------------------------------------------------------------------
-- Urna
---------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS stg_voto;
CREATE TABLE stg_voto(
		eleicao_id					INTEGER
,		local_id					INTEGER
,		participacao_id				INTEGER
,		votavel_id					INTEGER
,		urna_id						INTEGER
,		qt_votos					INTEGER
,		PRIMARY KEY(eleicao_id, local_id, participacao_id, votavel_id, urna_id)
);
*/