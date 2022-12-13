# -*- coding: utf-8 -*-
import argparse
import sys
import psycopg2
import googlemaps

from eleicoes2022.ingestion.repositories import IngestionRepository, CacheRepository
from eleicoes2022.ingestion.models import ZonaEleitoralCsv
from eleicoes2022.ingestion.services import IngestionService, CacheService, MapService
import eleicoes2022.lib.util as util
import eleicoes2022.config as config

logger = util.get_logger('geocode')

def build_deps():
    logger.info('assembling dependencies')
    conn = psycopg2.connect(config.POSTGRES_DSN)
    conn_cache = psycopg2.connect(config.POSTGRES_DSN)
    repo = IngestionRepository(conn)
    cache = CacheService(CacheRepository(conn_cache))
    client = googlemaps.Client(config.MAPS_APIKEY)
    geocode = MapService(client, cache)
    svc = IngestionService(repo, geocode)
    return conn, repo, svc
    
def main(csvpath, clear):
    conn, repo, svc = build_deps()
    logger.info('starting zona eleitoral ingestion tool')
    try:
        zonas = svc.read_csv_zonas(csvpath)
        if clear:
            svc.clear_staging_zonas_eleitorais()
        svc.load_staging_zonas_eleitorais(zonas)
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
    parser.add_argument('csvpath', type=str, help='input csv file path')
    args = parser.parse_args()
    main(args.csvpath, args.clear)