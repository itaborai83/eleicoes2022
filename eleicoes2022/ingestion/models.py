# -*- coding: utf-8 -*-
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, date, time
import eleicoes2022.ingestion.config as config
import eleicoes2022.lib.util as util

def parse_field(field_txt, type, strip_quotes=True):
    if strip_quotes:
        field_txt = field_txt[1:-1] # remove double quotes
    
    if type == int:
        if field_txt == "-1":
            return None
        return int(field_txt)
    
    elif type == str:
        if field_txt == "#NULO#":
            return None 
        return field_txt
         
    elif type == date:
        if field_txt == "#NULO#":
            return None 
        return datetime.strptime(field_txt, config.IMPORT_DATE_MASK).date()
        
    elif type == time:
        if field_txt == "#NULO#":
            return None 
        return datetime.strptime(field_txt, config.IMPORT_TIME_MASK).time()
    
    elif type == datetime:
        if field_txt == "#NULO#" or field_txt == '': # mal formed field??
            return None 
        return datetime.strptime(field_txt, config.IMPORT_DATETIME_MASK)
    raise ValueError(f"unexpected field type: {type}")

@dataclass
class ZonaEleitoralCsv:
    nr_zona                     : int  
    cod_processual              : str
    endereco                    : str
    cep                         : str
    bairro                      : str
    nome_municipio              : str
    sg_uf                       : str
    latitude                    : None
    longitude                   : None
    
    @classmethod
    def from_csv_row(klass, line):
        fields = util.parse_csv_line(line, sep=",", quote="\"")
        return klass(
            nr_zona                     = parse_field(fields[0].lstrip('0'), int, strip_quotes=False)  
        ,   cod_processual              = parse_field(fields[1], str, strip_quotes=False)
        ,   endereco                    = parse_field(fields[2], str, strip_quotes=False)
        ,   cep                         = parse_field(fields[3], str, strip_quotes=False)
        ,   bairro                      = parse_field(fields[4], str, strip_quotes=False)
        ,   nome_municipio              = parse_field(fields[5], str, strip_quotes=False)
        ,   sg_uf                       = parse_field(fields[6], str, strip_quotes=False)
        ,   latitude                    = None
        ,   longitude                   = None
        )    

@dataclass
class BoletimUrnaCsv:
    dt_geracao                  : date
    hh_geracao                  : time
    ano_eleicao                 : int
    cd_tipo_eleicao             : int
    nm_tipo_eleicao             : str
    cd_pleito                   : int
    dt_pleito                   : date
    nr_turno                    : int
    cd_eleicao                  : int
    ds_eleicao                  : str
    sg_uf                       : str
    cd_municipio                : int
    nm_municipio                : str
    nr_zona                     : int
    nr_secao                    : int
    nr_local_votacao            : int
    cd_cargo_pergunta           : int
    ds_cargo_pergunta           : str
    nr_partido                  : int
    sg_partido                  : str
    nm_partido                  : str
    dt_bu_recebido              : datetime
    qt_aptos                    : int
    qt_comparecimento           : int
    qt_abstencoes               : int
    cd_tipo_urna                : int
    ds_tipo_urna                : str
    cd_tipo_votavel             : int
    ds_tipo_votavel             : str
    nr_votavel                  : int
    nm_votavel                  : str
    qt_votos                    : int
    nr_urna_efetivada           : int
    cd_carga_1_urna_efetivada   : str
    cd_carga_2_urna_efetivada   : str
    cd_flashcard_urna_efetivada : str
    dt_carga_urna_efetivada     : datetime
    ds_cargo_pergunta_secao     : str
    ds_agregadas                : str
    dt_abertura                 : datetime
    dt_encerramento             : datetime
    qt_eleitores_biometria_nh   : int
    dt_emissao_bu               : datetime
    nr_junta_apuradora          : int
    nr_turma_apuradora          : int

    @classmethod
    def from_csv_row(klass, line):
        fields = line.split(";")
        return klass(
            dt_geracao                  = klass.parse_field(fields[ 0], date)
        ,   hh_geracao                  = klass.parse_field(fields[ 1], time)
        ,   ano_eleicao                 = klass.parse_field(fields[ 2], int)
        ,   cd_tipo_eleicao             = klass.parse_field(fields[ 3], int)
        ,   nm_tipo_eleicao             = klass.parse_field(fields[ 4], str)
        ,   cd_pleito                   = klass.parse_field(fields[ 5], int)
        ,   dt_pleito                   = klass.parse_field(fields[ 6], date)
        ,   nr_turno                    = klass.parse_field(fields[ 7], int)
        ,   cd_eleicao                  = klass.parse_field(fields[ 8], int)
        ,   ds_eleicao                  = klass.parse_field(fields[ 9], str)
        ,   sg_uf                       = klass.parse_field(fields[10], str)
        ,   cd_municipio                = klass.parse_field(fields[11], int)
        ,   nm_municipio                = klass.parse_field(fields[12], str)
        ,   nr_zona                     = klass.parse_field(fields[13], int)
        ,   nr_secao                    = klass.parse_field(fields[14], int)
        ,   nr_local_votacao            = klass.parse_field(fields[15], int)
        ,   cd_cargo_pergunta           = klass.parse_field(fields[16], int)
        ,   ds_cargo_pergunta           = klass.parse_field(fields[17], str)
        ,   nr_partido                  = klass.parse_field(fields[18], int)
        ,   sg_partido                  = klass.parse_field(fields[19], str)
        ,   nm_partido                  = klass.parse_field(fields[20], str)
        ,   dt_bu_recebido              = klass.parse_field(fields[21], datetime)
        ,   qt_aptos                    = klass.parse_field(fields[22], int)
        ,   qt_comparecimento           = klass.parse_field(fields[23], int)
        ,   qt_abstencoes               = klass.parse_field(fields[24], int)
        ,   cd_tipo_urna                = klass.parse_field(fields[25], int)
        ,   ds_tipo_urna                = klass.parse_field(fields[26], str)
        ,   cd_tipo_votavel             = klass.parse_field(fields[27], int)
        ,   ds_tipo_votavel             = klass.parse_field(fields[28], str)
        ,   nr_votavel                  = klass.parse_field(fields[29], int)
        ,   nm_votavel                  = klass.parse_field(fields[30], str)
        ,   qt_votos                    = klass.parse_field(fields[31], int)
        ,   nr_urna_efetivada           = klass.parse_field(fields[32], int)
        ,   cd_carga_1_urna_efetivada   = klass.parse_field(fields[33], str)
        ,   cd_carga_2_urna_efetivada   = klass.parse_field(fields[34], str)
        ,   cd_flashcard_urna_efetivada = klass.parse_field(fields[35], str)
        ,   dt_carga_urna_efetivada     = klass.parse_field(fields[36], datetime)
        ,   ds_cargo_pergunta_secao     = klass.parse_field(fields[37], str)
        ,   ds_agregadas                = klass.parse_field(fields[38], str)
        ,   dt_abertura                 = klass.parse_field(fields[39], datetime)
        ,   dt_encerramento             = klass.parse_field(fields[40], datetime)
        ,   qt_eleitores_biometria_nh   = klass.parse_field(fields[41], int)
        ,   dt_emissao_bu               = klass.parse_field(fields[42], datetime)
        ,   nr_junta_apuradora          = klass.parse_field(fields[43], int)
        ,   nr_turma_apuradora          = klass.parse_field(fields[44], int)  
        )
    
    @staticmethod
    def parse_field(field_txt, type):
        field_txt = field_txt[1:-1] # remove double quotes

        if type == int:
            if field_txt == "-1":
                return None
            return int(field_txt)
        
        elif type == str:
            if field_txt == "#NULO#":
                return None 
            return field_txt
             
        elif type == date:
            if field_txt == "#NULO#":
                return None 
            return datetime.strptime(field_txt, config.IMPORT_DATE_MASK).date()
            
        elif type == time:
            if field_txt == "#NULO#":
                return None 
            return datetime.strptime(field_txt, config.IMPORT_TIME_MASK).time()
        
        elif type == datetime:
            if field_txt == "#NULO#" or field_txt == '': # mal formed field??
                return None 
            #try:
            return datetime.strptime(field_txt, config.IMPORT_DATETIME_MASK)
            #except ValueError:
            #    return datetime.strptime(field_txt, config.IMPORT_DATE_MASK) # wtf?
        raise ValueError(f"unexpected field type: {type}")
    
    @staticmethod
    def to_pandas(boletins):
        return pd.DataFrame({
            'dt_geracao'                    : [ b.dt_geracao                    for b in boletins ]
        ,   'hh_geracao'                    : [ b.hh_geracao                    for b in boletins ]
        ,   'ano_eleicao'                   : [ b.ano_eleicao                   for b in boletins ]
        ,   'cd_tipo_eleicao'               : [ b.cd_tipo_eleicao               for b in boletins ]
        ,   'nm_tipo_eleicao'               : [ b.nm_tipo_eleicao               for b in boletins ]
        ,   'cd_pleito'                     : [ b.cd_pleito                     for b in boletins ]
        ,   'dt_pleito'                     : [ b.dt_pleito                     for b in boletins ]
        ,   'nr_turno'                      : [ b.nr_turno                      for b in boletins ]
        ,   'cd_eleicao'                    : [ b.cd_eleicao                    for b in boletins ]
        ,   'ds_eleicao'                    : [ b.ds_eleicao                    for b in boletins ]
        ,   'sg_uf'                         : [ b.sg_uf                         for b in boletins ]
        ,   'cd_municipio'                  : [ b.cd_municipio                  for b in boletins ]
        ,   'nm_municipio'                  : [ b.nm_municipio                  for b in boletins ]
        ,   'nr_zona'                       : [ b.nr_zona                       for b in boletins ]
        ,   'nr_secao'                      : [ b.nr_secao                      for b in boletins ]
        ,   'nr_local_votacao'              : [ b.nr_local_votacao              for b in boletins ]
        ,   'cd_cargo_pergunta'             : [ b.cd_cargo_pergunta             for b in boletins ]
        ,   'ds_cargo_pergunta'             : [ b.ds_cargo_pergunta             for b in boletins ]
        ,   'nr_partido'                    : [ b.nr_partido                    for b in boletins ]
        ,   'sg_partido'                    : [ b.sg_partido                    for b in boletins ]
        ,   'nm_partido'                    : [ b.nm_partido                    for b in boletins ]
        ,   'dt_bu_recebido'                : [ b.dt_bu_recebido                for b in boletins ]
        ,   'qt_aptos'                      : [ b.qt_aptos                      for b in boletins ]
        ,   'qt_comparecimento'             : [ b.qt_comparecimento             for b in boletins ]
        ,   'qt_abstencoes'                 : [ b.qt_abstencoes                 for b in boletins ]
        ,   'cd_tipo_urna'                  : [ b.cd_tipo_urna                  for b in boletins ]
        ,   'ds_tipo_urna'                  : [ b.ds_tipo_urna                  for b in boletins ]
        ,   'cd_tipo_votavel'               : [ b.cd_tipo_votavel               for b in boletins ]
        ,   'ds_tipo_votavel'               : [ b.ds_tipo_votavel               for b in boletins ]
        ,   'nr_votavel'                    : [ b.nr_votavel                    for b in boletins ]
        ,   'nm_votavel'                    : [ b.nm_votavel                    for b in boletins ]
        ,   'qt_votos'                      : [ b.qt_votos                      for b in boletins ]
        ,   'nr_urna_efetivada'             : [ b.nr_urna_efetivada             for b in boletins ]
        ,   'cd_carga_1_urna_efetivada'     : [ b.cd_carga_1_urna_efetivada     for b in boletins ]
        ,   'cd_carga_2_urna_efetivada'     : [ b.cd_carga_2_urna_efetivada     for b in boletins ]
        ,   'cd_flashcard_urna_efetivada'   : [ b.cd_flashcard_urna_efetivada   for b in boletins ]
        ,   'dt_carga_urna_efetivada'       : [ b.dt_carga_urna_efetivada       for b in boletins ]
        ,   'ds_cargo_pergunta_secao'       : [ b.ds_cargo_pergunta_secao       for b in boletins ]
        ,   'ds_agregadas'                  : [ b.ds_agregadas                  for b in boletins ]
        ,   'dt_abertura'                   : [ b.dt_abertura                   for b in boletins ]
        ,   'dt_encerramento'               : [ b.dt_encerramento               for b in boletins ]
        ,   'qt_eleitores_biometria_nh'     : [ b.qt_eleitores_biometria_nh     for b in boletins ]
        ,   'dt_emissao_bu'                 : [ b.dt_emissao_bu                 for b in boletins ]
        ,   'nr_junta_apuradora'            : [ b.nr_junta_apuradora            for b in boletins ]
        ,   'nr_turma_apuradora'            : [ b.nr_turma_apuradora            for b in boletins ]
        })