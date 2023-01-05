# -*- coding: utf-8 -*-
import sys
import os
import logging
import argparse
import dbm.gnu
import json
import googlemaps
import pandas as pd

MAPS_APIKEY         = os.environ['MAPS_APIKEY']
INPUT_FILE_SEP      = ";"
INPUT_FILE_ENCODING = "latin-1"

LOGGER_FORMAT = '%(asctime)s:%(levelname)s:%(filename)s:%(funcName)s:%(lineno)d\n\t%(message)s\n'
LOGGER_FORMAT = '%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s'
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT, handlers=[stdout_handler])
logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT)

logger = logging.getLogger('geocode')


class Cache:
    
    def __init__(self, cache_file):
        self.cache_file = cache_file
        self.db = None
        
    def open(self):
        assert self.db is None
        assert self.cache_file.endswith(".db")
        logger.info(f'opening cache file {self.cache_file}')
        self.db = dbm.gnu.open(self.cache_file, 'c')
    
    def get(self, key, default=None):
        assert self.db is not None
        bin_key = key.encode()
        logger.info(f'getting entry {key} from cache')
        value = self.db.get(bin_key, default)
        if value == default:
            logger.info(f'entry {key} not found in cache')
            return default
        logger.info(f'entry {key} found in cache')
        return json.loads(value)
    
    def set(self, key, value):
        assert self.db is not None
        logger.info(f'setting entry {key} on cache')
        bin_key = key.encode()
        self.db[bin_key] = json.dumps(value)
        self.db.sync()
    
    def delete(self, key):
        logger.info(f'removing entry {key} from cache')
        bin_key = key.encode()
        del self.db[bin_key]
        
    def close(self):
        assert self.db is not None
        logger.info(f'closing cache file {self.cache_file}')
        self.db.close()
        self.db = None

class MapService:
    
    def __init__(self, client, cache, cache_only=False):
        if client is None:
            cache_only = True
        self.client = client
        self.cache = cache
        self.cache_only = cache_only
    
    def _cache_key(self, address):
        return f"coords::({address})"
    
    def _api_call_key(self, address):
        return f"api-call::({address})"
    
    def _read_cached_coords(self, address):
        logger.info(f"reading cached coords")
        cache_key = self._cache_key(address)
        coords = self.cache.get(cache_key)
        if coords is not None:
            lat, lng = coords["lat"], coords["lng"]
            logger.info(f"found cached coords lat: {lat}, lng: {lng}")
            return True, (lat, lng)
        logger.info(f"cached coords not found")
        return False, None
        
    def _make_api_call(self, address):
        logger.info(f"making google maps api call")
        api_call_key = self._api_call_key(address)
        result = self.cache.get(api_call_key)
        if result is not None:
            logger.info(f"found cached api call result")
        else:
            result = self.client.geocode(address)
            self.cache.set(api_call_key, result)
        
        if len(result) == 0:
            logger.warn(f'failed geocoding result for {address}')
            self.update(address, -1, -1)
            return -1, -1
            
        lat = result[0]['geometry']['location']['lat']
        lng = result[0]['geometry']['location']['lng']
        self.update(address, lat, lng)
        return lat, lng
    
    def update(self, address, lat, lng):
        logger.info(f"updating address '{address}' to coords lat: {lat}, lng: {lng}")
        cache_key = self._cache_key(address)
        self.cache.set(cache_key, {"lat": lat, "lng": lng})
    
    def geocode(self, address):
        logger.info(f"geocoding address '{address}'")     
        ok, coords = self._read_cached_coords(address)
        if ok:
            lat, lng = coords
            return True, (lat, lng)
          
        if self.cache_only:
            raise ValueError("attempting to geocode on 'cache_only=True' mode")
        
        lat, lng = self._make_api_call(address)
        return True, (lat, lng)
        
class App:
    
    MAX_ROWS = 1000000
    
    def __init__(self, map_service):
        self.map_service = map_service
    
    def read_input_file(self, input_file):
        logger.info(f"reading input file '{input_file}'")
        eleitorado_df = pd.read_csv(input_file, sep=";", encoding="latin-1")
        logger.info(eleitorado_df.info())
        return eleitorado_df
    
    def run(self, input_file, output_file):
        logger.info('starting eleitorado geocoding tool')
        assert input_file.endswith(".csv")
        assert output_file.endswith(".parquet")
        eleitorado_df = self.read_input_file(input_file)
        seen = set()
        lats = []
        lngs = []
        for i, row in enumerate(eleitorado_df.itertuples()):
            if i > self.MAX_ROWS: 
                logger.error(f"max rows reached - {self.MAX_ROWS}")
                sys.exit(-1)
            address = f"{row.NM_BAIRRO} - {row.NM_MUNICIPIO} - {row.SG_UF}"
            was_seen = address in seen
            #if was_seen:
            #    continue
            lat, lng = float(row.NR_LATITUDE), float(row.NR_LONGITUDE)
            if (lat, lng) != (-1.0, -1.0):
                #logger.info(f'address {address} is already geocoded')
                if not was_seen:
                    self.map_service.update(address, lat, lng)
            else:
                #if not was_seen:
                ok, coords = self.map_service.geocode(address)
                logger.info(f'geocoding result: ok={ok} / coords={coords}')
                if ok:
                    lat, lng = coords
            logger.info(f"*** {i+1} *** > ({lat}, {lng}) {address}")
            lats.append(lat)
            lngs.append(lng)
            seen.add(address)
        logger.info('updating lat column')
        eleitorado_df['NR_LATITUDE'] = lats
        
        logger.info('updating lng column')
        eleitorado_df['NR_LONGITUDE'] = lngs
        
        logger.info(f'exporting file to parquet')
        eleitorado_df.to_parquet(output_file)
        
        logger.info('finished')

def main(input_file, output_file, cache_file):
    cache = Cache(cache_file)
    client = googlemaps.Client(MAPS_APIKEY)
    map_service = MapService(client, cache)
    try:
        app = App(map_service)
        cache.open()
        app.run(input_file, output_file)
    finally:
        cache.close()
   
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', type=str, help='eleitorado file: \nhttps://cdn.tse.jus.br/estatistica/sead/odsele/eleitorado_locais_votacao/eleitorado_local_votacao_2022.zip')
    parser.add_argument('output_file', type=str, help='output csv file')
    parser.add_argument('cache_file', type=str, help='cache file')
    args = parser.parse_args()
    main(args.input_file, args.output_file, args.cache_file)