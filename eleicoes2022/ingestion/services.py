# -*- coding: utf-8 -*-
import pandas as pd
from eleicoes2022.ingestion.models import BoletimUrnaCsv
import eleicoes2022.ingestion.config as config
import eleicoes2022.lib.util as util

logger = util.get_logger('ingestion.services')

class IngestionService:
    
    def __init__(self, repo):
        self.repo = repo

    def read_csv(self, filepath):
        logger.info(f"loading staging table from {filepath}")
        boletins = []
        logger.info(f"parsing records from csv file")
        records = 0
        with open(filepath, encoding=config.CSV_ENCODING) as fh:
            fh.readline() # remove csv header
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    boletim = BoletimUrnaCsv.from_csv_row(line)
                except:
                    logger.exception('error parsing csv line')
                    logger.error(line)
                    raise
                boletins.append(boletim)
                records += 1
                if records % 10000 == 0:
                    logger.info(f"{records} records read so far")
        logger.info(f"{len(boletins)} records read")
        return boletins
        
    def clear_staging(self):
        logger.info("clearing staging table")
        self.repo.clear_staging()

    def load_staging(self, boletins):
        logger.info("loading table")
        self.repo.insert_boletins(boletins)