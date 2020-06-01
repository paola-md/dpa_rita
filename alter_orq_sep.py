# PYTHONPATH='.' AWS_PROFILE=educate1 luigi --module alter_orq downloadDataS3 --local-scheduler

# PARA UN MODELO
#PYTHONPATH='.' AWS_PROFILE=dpa luigi --module alter_orq  RunModel --local-scheduler  --bucname models-dpa --numIt 2 --numPCA 3  --model LR --obj 0-1.5

# PARA TODOS LOS MODELOS
# PYTHONPATH='.' AWS_PROFILE=dpa luigi --module alter_orq  RunAllTargets --local-scheduler  --bucname models-dpa --numIt 1 --numPCA 2  --model LR

# CENTRAL scheduler
#PYTHONPATH='.' AWS_PROFILE=dpa luigi --module alter_orq  RunAllTargets  --bucname models-dpa --numIt 1 --numPCA 2  --model LR

###  Librerias necesarias
import luigi
import luigi.contrib.s3
from luigi import Event, Task, build # Utilidades para acciones tras un task exitoso o fallido
from luigi.contrib.postgres import CopyToTable,PostgresQuery
import boto3
from datetime import date, datetime
import getpass # Usada para obtener el usuario
from io import BytesIO
import socket #import publicip
import requests
import os, subprocess, ast
import pandas as pd
import psycopg2
from psycopg2 import extras
from zipfile import ZipFile
from pathlib import Path
###librerias para clean
from pyspark.sql import SparkSession
from src.features.build_features import clean, crear_features

###  Imports desde directorio de proyecto dpa_rita
## Credenciales
from src import(
MY_USER,
MY_PASS,
MY_HOST,
MY_PORT,
MY_DB,
)

## Utilidades
from src.utils.s3_utils import create_bucket
from src.utils.db_utils import create_db, execute_sql, save_rds
from src.utils.ec2_utils import create_ec2
from src.utils.metadatos_utils import EL_verif_query, EL_metadata, Linaje_raw,EL_load,clean_metadata_rds
from src.utils.metadatos_utils import Linaje_clean_data, Linaje_semantic, semantic_metadata, Insert_to_RDS
from src.utils.metadatos_utils import rita_light_query,Linaje_load,load_verif_query
from src.utils.db_utils import execute_sql
from src.models.train_model import run_model
from src.models.save_model import parse_filename

#Pruebas unitarias Load
#from tasks.load_test import Load_Testing
from testing.test_absent_hearders import TestingHeaders

#Metadata Clean Testing
from src.utils.metadatos_utils import C_testing_clean_columns,C_testing_clean_rangos
from src.utils.metadatos_utils import Linaje_clean_columns_testing, Linaje_clean_rangos_testing

#Pruebas unitarias Clean
from testing.test_clean_columns import Test_Columns_Case
from testing.test_clean_rangos import Test_Ranges_Case

#Metadata Semantic Testing
from src.utils.metadatos_utils import Linaje_extract_testing, EL_testing_extract
from src.utils.metadatos_utils import Linaje_load_testing, EL_testing_load
from src.utils.metadatos_utils import Linaje_semantic1_testing, Linaje_semantic2_testing, FE_testing_semantic

#Pruebas unitarias Semantic
from testing.test_semantic_columns import TestSemanticColumns
from testing.test_semantic_column_types import TestSemanticColumnsTypes


# Dependencias de Tasks previos =======
from tasks.extract import Extraction
from tasks.metadatos_extract import Metadata_Extract
from tasks.load_test import Load_Testing
from tasks.load import Load
from tasks.metadatos_load import Metadata_Load
from tasks.clean_column_testing import CleanColumn_Testing
from tasks.clean_rango_testing import CleanRango_Testing
from tasks.clean import GetCleanData
from tasks.metadatos_clean import Metadata_Clean
from tasks.semantic_column_testing import Semantic_Testing_col
from tasks.semantic_type_testing import Semantic_Testing
from tasks.semantic import GetFEData
from tasks.metadatos_semantic import Metadata_Semantic
from tasks.bucket import CreateModelBucket
#from tasks.modeling import RunModel
from src.orquestadores.modelling import RunModelSimple, RunModel,\
RunTargetA, RunTargetB, RunTargetC, RunTargetD

# Listas auxiliares
meta_extract = [] # arreglo para reunir tuplas de metadatos
meta_load = [] # arreglo para reunir tuplas de metadatos
meta_clean = [] # arreglo para reunir tuplas de metadatos
meta_semantic = [] # arreglo para reunir tuplas de metadatos
CURRENT_DIR = os.getcwd()


# =======================================================
# ALL TARGETS
# 1) "0-1.5"
# 2) "1.5-3.5"
# 3) "3.5-"
# 4) "cancelled"
# =======================================================
TARGET_A = "0-1.5"
TARGET_B = "1.5-3.5"
TARGET_C =  "3.5-"
TARGET_D = "cancelled"
