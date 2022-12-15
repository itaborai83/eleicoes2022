# -*- coding: utf-8 -*-
import json
import zipfile
import pandas as pd
from eleicoes2022.ingestion.models import ZonaEleitoralCsv, BoletimUrnaCsv
import eleicoes2022.ingestion.config as config
import eleicoes2022.lib.util as util


logger = util.get_logger('ingestion.services')

class IngestionService:
    
    CSV_ENCODING = "latin-1"
    
    def __init__(self, repo, map_svc):
        self.repo = repo
        self.map_svc = map_svc
    
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
        zonas = list(zonas)
        result = self.map_svc.geocode("Brasil")
        for zona in zonas:
            address  = zona.geocode_address()
            logger.info(f"geocoding address: {address}")
            result = self.map_svc.geocode(address)
            zona.endereco_formatado = result[0]['formatted_address']
            zona.latitude = result[0]['geometry']['location']['lat']
            zona.longitude = result[0]['geometry']['location']['lng']
        self.repo.insert_zonas_eleitorais(zonas, commit=commit)

class CacheService:

    def __init__(self, repo):
        self.repo = repo
        
    def put(self, entry_type, key, value):
        return self.repo.put(entry_type, key, value)
    
    def get(self, entry_type, key):
        return self.repo.get(entry_type, key)
    
    def expire_stale(self):
        return self.repo.expire_stale()
        
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback()

class MapService:
    
    ENTRY_TYPE =  "MAPS::GEOCODE"
    
    def __init__(self, client, cache):
        self.client = client
        self.cache = cache
    
    def geocode(self, address):
        result = self.cache.get(self.ENTRY_TYPE, address)
        if result:
            return json.loads(result)
        result = self.client.geocode(address)
        self.cache.put(self.ENTRY_TYPE, address, json.dumps(result))
        