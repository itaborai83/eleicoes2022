# -*- coding: utf-8 -*-
import argparse
import sys
import sqlite3

from eleicoes2022.ingestion.repositories import BoletimRepository
from eleicoes2022.ingestion.models import BoletimUrnaCsv
from eleicoes2022.ingestion.services import IngestionService
import eleicoes2022.lib.util as util

logger = util.get_logger('ingestcsv')

def build_deps(dbfile):
    logger.info('assembling dependencies')
    conn = sqlite3.connect(dbfile)
    repo = BoletimRepository(conn)
    svc = IngestionService(repo)
    return conn, repo, svc
    
def main(csvpath, dbfile, clear):
    conn, repo, svc = build_deps(dbfile)
    logger.info('starting csv ingestion tool')
    try:
        boletins = svc.read_csv(csvpath)
        if clear:
            svc.clear_staging()
        svc.load_staging(boletins)
        repo.commit()
    except:
        logger.exception('a fatal error has occurred')
        sys.exit(-1)
    finally:
        conn.close()
    logger.info('finished')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--clear', action='store_true', help='purge staging table first')
    parser.add_argument('csvpath', type=str, help='input csv file path')
    parser.add_argument('dbfile', type=str, help='sqlite db file')
    args = parser.parse_args()
    main(
        args.csvpath
    ,   args.dbfile
    ,   args.clear
    )