---------------------------------------------------------------------------------------------------
-- Boletim
---------------------------------------------------------------------------------------------------
-- DROP TABLE stg_boletim
CREATE TABLE IF NOT EXISTS stg_boletim(
	dt_geracao						TEXT
,	hh_geracao                      TEXT
,	ano_eleicao                     INTEGER
,	cd_tipo_eleicao                 INTEGER
,	nm_tipo_eleicao                 TEXT
,	cd_pleito                       INTEGER
,	dt_pleito                       TEXT
,	nr_turno                        INTEGER
,	cd_eleicao                      INTEGER
,	ds_eleicao                      TEXT
,	sg_uf                           TEXT
,	cd_municipio                    INTEGER
,	nm_municipio                    TEXT
,	nr_zona                         INTEGER
,	nr_secao                        INTEGER
,	nr_local_votacao                INTEGER
,	cd_cargo_pergunta               INTEGER
,	ds_cargo_pergunta               TEXT
,	nr_partido                      INTEGER
,	sg_partido                      TEXT
,	nm_partido                      TEXT
,	dt_bu_recebido                  TEXT
,	qt_aptos                        INTEGER
,	qt_comparecimento               INTEGER
,	qt_abstencoes                   INTEGER
,	cd_tipo_urna                    INTEGER
,	ds_tipo_urna                    TEXT
,	cd_tipo_votavel                 INTEGER
,	ds_tipo_votavel                 TEXT
,	nr_votavel                      INTEGER
,	nm_votavel                      TEXT
,	qt_votos                        INTEGER
,	nr_urna_efetivada               INTEGER
,	cd_carga_1_urna_efetivada       TEXT
,	cd_carga_2_urna_efetivada       TEXT
,	cd_flashcard_urna_efetivada     TEXT
,	dt_carga_urna_efetivada         TEXT
,	ds_cargo_pergunta_secao         TEXT
,	ds_agregadas                    TEXT
,	dt_abertura                     TEXT
,	dt_encerramento                 TEXT
,	qt_eleitores_biometria_nh       INTEGER
,	dt_emissao_bu                   TEXT
,	nr_junta_apuradora				INTEGER
,	nr_turma_apuradora				INTEGER
);

-- DROP TABLE stg_zonas_eleitorais
CREATE TABLE IF NOT EXISTS stg_zonas_eleitorais(
	sg_uf                       	TEXT
,   nr_zona                     	INTEGER  
,   cod_processual              	TEXT
,   endereco                    	TEXT
,   cep                         	TEXT
,   bairro                      	TEXT
,   nome_municipio              	TEXT
,	endereco_formatado				TEXT
,	longitude						FLOAT
,	latitude                        FLOAT
);

-- DROP TABLE cached_values
CREATE TABLE IF NOT EXISTS cached_values(
   	entry_type                  	TEXT
,   cache_key                   	TEXT
,   cache_value                 	TEXT
,   cached_at                   	TEXT
,   expire_at                   	TEXT
,	PRIMARY KEY(entry_type, cache_key)
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