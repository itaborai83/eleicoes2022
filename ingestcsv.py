# -*- coding: utf-8 -*-
import argparse
import sys
import psycopg2


from eleicoes2022.ingestion.repositories import BoletimRepository
from eleicoes2022.ingestion.models import BoletimUrnaCsv
from eleicoes2022.ingestion.services import IngestionService
import eleicoes2022.lib.util as util
import eleicoes2022.config as config

logger = util.get_logger('ingestcsv')

def build_deps():
    logger.info('assembling dependencies')
    conn = psycopg2.connect(config.POSTGRES_DSN)
    repo = BoletimRepository(conn)
    svc = IngestionService(repo)
    return conn, repo, svc
    
def main(csvpath, clear):
    conn, repo, svc = build_deps()
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
    args = parser.parse_args()
    main(args.csvpath, args.clear)