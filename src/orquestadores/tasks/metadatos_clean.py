import luigi.contrib.postgres
from pathlib import Path
import pandas as pd
import os

from src.orquestadores.tasks.clean import GetCleanData

###  Imports desde directorio de proyecto dpa_rita
## Credenciales
from src import(
MY_USER,
MY_PASS,
MY_HOST,
MY_PORT,
MY_DB,
)

# ======================================================
# Metadatos de etapa load
# ======================================================

class Metadata_Clean(luigi.contrib.postgres.CopyToTable):
    '''
    Task de luigi para insertar renglones en tabla de metadatos de clean
    '''
    def requires(self):
        return GetCleanData()

    # Lectura de archivo de credenciales
    user = MY_USER
    password = MY_PASS
    database = MY_DB
    host = MY_HOST

    # Nombre de tabla donde se inserta info. Notas:
    # 1) si la tabla (sin esquema) no existe, luigi la crea con esquema publico,
    # 2) si el esquema de la tabla no existe, luigi devuelve error :(
    table = 'metadatos.clean'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns = [("fecha", "VARCHAR"),\
            ("nombre_task", "VARCHAR"),\
            ("usuario","VARCHAR"),\
            ("ip_ec2","VARCHAR"),\
            ("num_columnas_modificadas","VARCHAR"),\
            ("num_filas_modificadas","VARCHAR"),\
            ("variables_limpias", "VARCHAR"),\
            ("task_status", "VARCHAR")]

    def rows(self):
        # Funcion para insertar renglones en tabla

        # Renglon o renglones (separados por coma) a ser insertado
        for data_file in Path('metadata').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file, header=None)

                # Insertamos renglones en tabla
                for element in reader.itertuples(index=False):
                    yield element

                os.system('rm metadata/*.csv')

        print('\n--- Carga de metadatos de clean realizada con exito ---\n')
