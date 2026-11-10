"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import schedule
import time
import json
from typing import Dict, List, Tuple

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'fleetlogix',
    'user': 'postgres',
    'password': 'your_password',
    'port': 5432
}

SNOWFLAKE_CONFIG = {
    'user': 'your_user',
    'password': 'your_password',
    'account': 'your_account',
    'warehouse': 'FLEETLOGIX_WH',
    'database': 'FLEETLOGIX_DW',
    'schema': 'ANALYTICS'
}

def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info(" Conectado a PostgreSQL")
            
            # Snowflake
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info(" Conectado a Snowflake")
            
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False