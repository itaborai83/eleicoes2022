import os
import sqlalchemy
POSTGRES_DSN            = os.environ['POSTGRES_DSN']
POSTGRES_SQLALCHEMY_DSN = os.environ['POSTGRES_SQLALCHEMY_DSN']
POSTGRES_ENGINE         = sqlalchemy.create_engine(POSTGRES_SQLALCHEMY_DSN)
MAPS_APIKEY             = os.environ['MAPS_APIKEY']