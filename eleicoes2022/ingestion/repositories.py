# -*- coding: utf-8 -*-
from textwrap import dedent
import eleicoes2022.ingestion.config as config
import eleicoes2022.lib.util as util

logger = util.get_logger('ingestion.repositories')

class BaseBoletimRepository:

    def clear_staging(self):
        raise NotImplementedError
    
    def load_staging(self, boletins):
        raise NotImplementedError
    
class BoletimRepository(BaseBoletimRepository):
    
    BATCH_SIZE = 10000
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
            %s -- dt_geracao
        ,   %s -- hh_geracao
        ,   %s -- ano_eleicao
        ,   %s -- cd_tipo_eleicao
        ,   %s -- nm_tipo_eleicao
        ,   %s -- cd_pleito
        ,   %s -- dt_pleito
        ,   %s -- nr_turno
        ,   %s -- cd_eleicao
        ,   %s -- ds_eleicao
        ,   %s -- sg_uf
        ,   %s -- cd_municipio
        ,   %s -- nm_municipio
        ,   %s -- nr_zona
        ,   %s -- nr_secao
        ,   %s -- nr_local_votacao
        ,   %s -- cd_cargo_pergunta
        ,   %s -- ds_cargo_pergunta
        ,   %s -- nr_partido
        ,   %s -- sg_partido
        ,   %s -- nm_partido
        ,   %s -- dt_bu_recebido
        ,   %s -- qt_aptos
        ,   %s -- qt_comparecimento
        ,   %s -- qt_abstencoes
        ,   %s -- cd_tipo_urna
        ,   %s -- ds_tipo_urna
        ,   %s -- cd_tipo_votavel
        ,   %s -- ds_tipo_votavel
        ,   %s -- nr_votavel
        ,   %s -- nm_votavel
        ,   %s -- qt_votos
        ,   %s -- nr_urna_efetivada
        ,   %s -- cd_carga_1_urna_efetivada
        ,   %s -- cd_carga_2_urna_efetivada
        ,   %s -- cd_flashcard_urna_efetivada
        ,   %s -- dt_carga_urna_efetivada
        ,   %s -- ds_cargo_pergunta_secao
        ,   %s -- ds_agregadas
        ,   %s -- dt_abertura
        ,   %s -- dt_encerramento
        ,   %s -- qt_eleitores_biometria_nh
        ,   %s -- dt_emissao_bu
        ,   %s -- nr_junta_apuradora
        ,   %s -- nr_turma_apuradora
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
        cursor = self.conn.cursor()
        for i, boletim in enumerate(boletins):
            if (i + 1) % self.BATCH_SIZE == 0:
                logger.info(f"inserting batch {i+1}")
                cursor.executemany(self.IMPORT_BOLETIM_SQL, acc)
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
            logger.info(f"inserting last batch")
            cursor.executemany(self.IMPORT_BOLETIM_SQL, acc)
            acc.clear()
        cursor.close()