# -*- coding: utf-8 -*-
from textwrap import dedent

import eleicoes2022.ingestion.config as config

class BaseBoletimRepository:

    def clear_staging(self):
        raise NotImplementedError
    
    def load_staging(self, boletins):
        raise NotImplementedError
    
class BoletimRepository(BaseBoletimRepository):
    
    CLEAR_STAGING_SQL = "DELETE FROM stg_boletim"
    IMPORT_BOLETIM_SQL = dedent("""\
        INSERT INTO stg_boletim(
            dt_geracao
        ,   hh_geracao
        ,   ano_eleicao
        ,   cd_tipo_eleicao
        ,   nm_tipo_eleicao
        ,   cd_pleito
        ,   dt_pleito
        ,   nr_turno
        ,   cd_eleicao
        ,   ds_eleicao
        ,   sg_uf
        ,   cd_municipio
        ,   nm_municipio
        ,   nr_zona
        ,   nr_secao
        ,   nr_local_votacao
        ,   cd_cargo_pergunta
        ,   ds_cargo_pergunta
        ,   nr_partido
        ,   sg_partido
        ,   nm_partido
        ,   dt_bu_recebido
        ,   qt_aptos
        ,   qt_comparecimento
        ,   qt_abstencoes
        ,   cd_tipo_urna
        ,   ds_tipo_urna
        ,   cd_tipo_votavel
        ,   ds_tipo_votavel
        ,   nr_votavel
        ,   nm_votavel
        ,   qt_votos
        ,   nr_urna_efetivada
        ,   cd_carga_1_urna_efetivada
        ,   cd_carga_2_urna_efetivada
        ,   cd_flashcard_urna_efetivada
        ,   dt_carga_urna_efetivada
        ,   ds_cargo_pergunta_secao
        ,   ds_agregadas
        ,   dt_abertura
        ,   dt_encerramento
        ,   qt_eleitores_biometria_nh
        ,   dt_emissao_bu
        ,   nr_junta_apuradora
        ,   nr_turma_apuradora
        ) VALUES (
            ? -- dt_geracao
        ,   ? -- hh_geracao
        ,   ? -- ano_eleicao
        ,   ? -- cd_tipo_eleicao
        ,   ? -- nm_tipo_eleicao
        ,   ? -- cd_pleito
        ,   ? -- dt_pleito
        ,   ? -- nr_turno
        ,   ? -- cd_eleicao
        ,   ? -- ds_eleicao
        ,   ? -- sg_uf
        ,   ? -- cd_municipio
        ,   ? -- nm_municipio
        ,   ? -- nr_zona
        ,   ? -- nr_secao
        ,   ? -- nr_local_votacao
        ,   ? -- cd_cargo_pergunta
        ,   ? -- ds_cargo_pergunta
        ,   ? -- nr_partido
        ,   ? -- sg_partido
        ,   ? -- nm_partido
        ,   ? -- dt_bu_recebido
        ,   ? -- qt_aptos
        ,   ? -- qt_comparecimento
        ,   ? -- qt_abstencoes
        ,   ? -- cd_tipo_urna
        ,   ? -- ds_tipo_urna
        ,   ? -- cd_tipo_votavel
        ,   ? -- ds_tipo_votavel
        ,   ? -- nr_votavel
        ,   ? -- nm_votavel
        ,   ? -- qt_votos
        ,   ? -- nr_urna_efetivada
        ,   ? -- cd_carga_1_urna_efetivada
        ,   ? -- cd_carga_2_urna_efetivada
        ,   ? -- cd_flashcard_urna_efetivada
        ,   ? -- dt_carga_urna_efetivada
        ,   ? -- ds_cargo_pergunta_secao
        ,   ? -- ds_agregadas
        ,   ? -- dt_abertura
        ,   ? -- dt_encerramento
        ,   ? -- qt_eleitores_biometria_nh
        ,   ? -- dt_emissao_bu
        ,   ? -- nr_junta_apuradora
        ,   ? -- nr_turma_apuradora
        ); 
    """)
    
    def __init__(self, conn):
        self.conn = conn
    
    def begin(self):
        self.conn.begin()
        
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback()
        
    def clear_staging(self):
        self.conn.execute(self.CLEAR_STAGING_SQL)
    
    def insert_boletins(self, boletins):
        acc = []
        for i, boletim in enumerate(boletins):
            if (i + 1) % 10000 == 0:
                self.conn.executemany(self.IMPORT_BOLETIM_SQL, acc)
                acc.clear()
            params = (
                boletim.dt_geracao.strftime(config.DATE_MASK)
            ,   boletim.hh_geracao.strftime(config.TIME_MASK)
            ,   boletim.ano_eleicao
            ,   boletim.cd_tipo_eleicao
            ,   boletim.nm_tipo_eleicao
            ,   boletim.cd_pleito
            ,   boletim.dt_pleito.strftime(config.DATETIME_MASK)
            ,   boletim.nr_turno
            ,   boletim.cd_eleicao
            ,   boletim.ds_eleicao
            ,   boletim.sg_uf
            ,   boletim.cd_municipio
            ,   boletim.nm_municipio
            ,   boletim.nr_zona
            ,   boletim.nr_secao
            ,   boletim.nr_local_votacao
            ,   boletim.cd_cargo_pergunta
            ,   boletim.ds_cargo_pergunta
            ,   boletim.nr_partido
            ,   boletim.sg_partido
            ,   boletim.nm_partido
            ,   boletim.dt_bu_recebido
            ,   boletim.qt_aptos
            ,   boletim.qt_comparecimento
            ,   boletim.qt_abstencoes
            ,   boletim.cd_tipo_urna
            ,   boletim.ds_tipo_urna
            ,   boletim.cd_tipo_votavel
            ,   boletim.ds_tipo_votavel
            ,   boletim.nr_votavel
            ,   boletim.nm_votavel
            ,   boletim.qt_votos
            ,   boletim.nr_urna_efetivada
            ,   boletim.cd_carga_1_urna_efetivada
            ,   boletim.cd_carga_2_urna_efetivada
            ,   boletim.cd_flashcard_urna_efetivada
            ,   boletim.dt_carga_urna_efetivada.strftime(config.DATETIME_MASK)
            ,   boletim.ds_cargo_pergunta_secao
            ,   boletim.ds_agregadas
            ,   boletim.dt_abertura.strftime(config.DATETIME_MASK) if boletim.dt_abertura else None
            ,   boletim.dt_encerramento.strftime(config.DATETIME_MASK) if boletim.dt_abertura else None
            ,   boletim.qt_eleitores_biometria_nh
            ,   boletim.dt_emissao_bu.strftime(config.DATETIME_MASK)
            ,   boletim.nr_junta_apuradora
            ,   boletim.nr_turma_apuradora
            )
            acc.append(params)
        if len(acc) > 0:
            self.conn.executemany(self.IMPORT_BOLETIM_SQL, acc)
            acc.clear()