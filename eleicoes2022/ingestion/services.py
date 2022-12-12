# -*- coding: utf-8 -*-
import zipfile
import pandas as pd
from eleicoes2022.ingestion.models import ZonaEleitoralCsv, BoletimUrnaCsv
import eleicoes2022.ingestion.config as config
import eleicoes2022.lib.util as util


logger = util.get_logger('ingestion.services')

class IngestionService:
    
    CSV_ENCODING = "latin-1"
    
    def __init__(self, repo):
        self.repo = repo
    
    def parse_boletim(self, buffer):
        line = buffer.decode(self.CSV_ENCODING)
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        try:
            return BoletimUrnaCsv.from_csv_row(line)
        except:
            logger.exception('error parsing csv line')
            logger.error(line)
            raise
    
    def parse_zona(self, line):
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        try:
            return ZonaEleitoralCsv.from_csv_row(line)
        except:
            logger.exception('error parsing csv line')
            logger.error(line)
            raise
    
    def read_csv_boletins(self, fh):
        records = 0
        fh.readline() # strip header
        for buffer in fh.readlines():
            boletim = self.parse_boletim(buffer)
            if boletim is None:
                continue
            yield boletim
            records += 1
            if records % 10000 == 0:
                logger.info(f"{records} records read so far")
        logger.info(f"{records} records read")
        
    def read_zip_boletins(self, filepath):
        logger.info(f"loading staging table from {filepath}")
        logger.info(f"parsing records from csv file")
        with zipfile.ZipFile(filepath) as zip:
            # skip pdf file
            for file in zip.filelist:
                if not file.filename.endswith('.csv'):
                    continue
                
                with zip.open(file) as fh:
                    for boletim in self.read_csv_boletins(fh):
                        yield boletim
    
    def read_csv_zonas(self, filepath):
        logger.info(f"loading staging table from {filepath}")
        logger.info(f"parsing records from csv file")
        with open(filepath, encoding=self.CSV_ENCODING) as fh:
            records = 0
            fh.readline() # strip header
            for line in fh.readlines():
                zona = self.parse_zona(line)
                if zona is None:
                    continue
                yield zona
                records += 1
                if records % 10000 == 0:
                    logger.info(f"{records} records read so far")
            logger.info(f"{records} records read")
        
    def clear_staging_boletins(self):
        logger.info("clearing staging table boletins")
        self.repo.clear_staging_boletins()

    def clear_staging_zonas_eleitorais(self):
        logger.info("clearing staging table zonas eleitorais")
        self.repo.clear_staging_zonas_eleitorais()

    def load_staging_boletins(self, boletins, commit=True):
        logger.info("loading table")
        self.repo.insert_boletins(boletins, commit=commit)
        
    def load_staging_zonas_eleitorais(self, zonas, commit=True):
        logger.info("loading table")
        self.repo.insert_zonas_eleitorais(zonas, commit=commit)