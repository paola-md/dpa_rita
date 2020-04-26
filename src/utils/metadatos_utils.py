from luigi.contrib.postgres import CopyToTable
import pandas as pd
import luigi
import psycopg2


from src import(
MY_USER,
MY_PASS,
MY_HOST,
MY_PORT,
MY_DB,
)


# ===============================
# Clases para reunir metadatos
# ===============================

## Clase para reunir los metadatos de la etapa Raw (tabla metadatos.raw)
class Linaje_raw():
    def __init__(self, url = 0, fecha=0, year=0, month=0, usuario=0, ip_ec2=0,\
     tamano_zip=0, nombre_archivo=0, ruta_s3=0,task_status=0):
        self.url = url
        self.fecha = fecha # time stamp
        self.nombre_task = self.__class__.__name__#nombre_task
        self.year = year #
        self.month = month #
        self.usuario = usuario # Usuario de la maquina de GNU/Linux que corre la instancia
        self.ip_ec2 = ip_ec2
        self.tamano_zip = tamano_zip
        self.nombre_archivo = nombre_archivo
        self.ruta_s3= ruta_s3
        self.task_status= task_status

    def to_upsert(self):
        return (self.fecha, self.nombre_task, self.year, self.month, self.usuario,\
         self.ip_ec2, self.tamano_zip, self.nombre_archivo, self.ruta_s3,\
          self.task_status)

# ==========================================
# Funciones para insertar metadatos a RDS
# ==========================================

class InsertExtractMetada(CopyToTable):
    '''
    Task de luigi para insertar renglones en renglones en tabla de metadatos
    de la extraccion y load de metadatos a S3 (no usada)
    '''

    # Lectura de archivo de credenciales
    user = MY_USER
    password = MY_PASS
    database = MY_DB
    host = MY_HOST

    # Nombre de tabla donde se inserta info. Notas:
    # 1) si la tabla (sin esquema) no existe, luigi la crea con esquema publico,
    # 2) si el esquema de la tabla no existe, luigi devuelve error :(
    table = 'metadatos.extract'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns = [("fecha", "VARCHAR"),\
            ("nombre_task", "VARCHAR"),\
            ("year","VARCHAR"),\
            ("month","VARCHAR"),\
            ("usuario","VARCHAR"),\
            ("ip_ec2","VARCHAR"),\
            ("tamano_zip","VARCHAR"),\
            ("nombre_archivo","VARCHAR"),\
            ("ruta_s3","VARCHAR"),\
            ("task_status", "VARCHAR")]

    def rows(self):
        # Funcion para insertar renglones en tabla

        # Renglon o renglones (separados por coma) a ser insertado
        r = [MiLinaje.to_upsert()]

        # Insertamos renglones en tabla
        for element in r:
            yield element

def EL_verif_query(url,anio,mes):
    '''
    Funcion para verificar si cierto month y year ya estan en metadatos.extract
    considerando el tamanio resultante de un query
    '''
    # Conexion y cursor para query
    connection = psycopg2.connect(user = MY_USER, # Usuario RDS
                                 password = MY_PASS, # password de usuario de RDS
                                 host = MY_HOST,# endpoint
                                 port="5432", # cambiar por el puerto
                                 database=MY_DB) # Nombre de la base de datos
    cursor = connection.cursor()

    # Query para verificacion a la base de datos
    postgreSQL_select_Query = "SELECT * from metadatos.extract WHERE year = '" + str(anio) + "' AND month = '"+ str(mes)+"';"
    cursor.execute(postgreSQL_select_Query)
    #print("Query de verificacion "+str(anio)+"/"+"str(mes)")
    select_Query = cursor.fetchall()
    tam = len(select_Query)
    cursor.close()
    connection.close()
    #print("PostgreSQL connection is closed")

    return tam

def EL_metadata(record_to_insert):
    '''
    Funcion para insertar metadatos de cierto month y year a metados.extract
    '''
    # Conexion y cursor para query
    connection = psycopg2.connect(user = MY_USER, # Usuario RDS
                                 password = MY_PASS, # password de usuario de RDS
                                 host = MY_HOST,# endpoint
                                 port="5432", # cambiar por el puerto
                                 database=MY_DB) # Nombre de la base de datos
    cursor = connection.cursor()

    # Query para insertar metadatos
    postgres_insert_query = """ INSERT INTO metadatos.extract (fecha, nombre_task,\
     year, month, usuario, ip_ec2, tamano_zip, nombre_archivo, ruta_s3, \
     task_status) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) """
    cursor.execute(postgres_insert_query, record_to_insert)
    connection .commit()
    cursor.close()
    connection.close()

    return print("Metadadata Insertion Done - PostgreSQL connection is closed")


def EL_rawdata():
    '''
    Funcion auxiliar para insertar a raw.rita (no usada por problemas de permisos en RDS)
    '''
    # Conexion y cursor para query
    connection = psycopg2.connect(user = MY_USER, # Usuario RDS
                                 password = MY_PASS, # password de usuario de RDS
                                 host = MY_HOST,# endpoint
                                 port="5432", # cambiar por el puerto
                                 database=MY_DB) # Nombre de la base de datos
    cursor = connection.cursor()

    # Query para insertar datos a raw
    #csv_file_name = 'data.csv'
    #sql = "COPY raw.rita FROM STDIN DELIMITER ',' CSV HEADER"
    #cursor.copy_expert(sql, open(csv_file_name, "r"))

    #f = open('data.csv', 'r')
    #cursor.copy_from(f, 'raw.rita', sep=",")
    postgres_insert_query = """copy raw.rita FROM 'data.csv' WITH CSV HEADER;"""
    cursor.execute(postgres_insert_query)
    connection .commit()
    #
    cursor.close()
    connection.close()

    return print("Raw.Rita insertion Done - PostgreSQL connection is closed")


 #-------------------------------------------------------------------------------------------------------------       

# Preparamamos una clase para reunir los metadatos de la etapa Raw
class Linaje_semantic():
    def __init__(self, url = 0, fecha=0, year=0, month=0, usuario=0, ip_ec2=0, filas_modificadas=0, variables=0, ruta_s3=0,task_status=0):
        self.url = url
        self.fecha = fecha # time stamp
        self.nombre_task = self.__class__.__name__#nombre_task
        self.year = year #
        self.month = month #
        self.usuario = usuario # Usuario de la maquina de GNU/Linux que corre la instancia
        self.ip_ec2 = ip_ec2
        self.filas_modificadas = filas_modificadas
        self.variables = variables
        self.ruta_s3= ruta_s3
        self.task_status= task_status

    def to_upsert(self):
        return (self.fecha, self.nombre_task, self.year, self.month, self.usuario,\
         self.ip_ec2, self.filas_modificadas, self.variables, self.ruta_s3,\
          self.task_status)

def semantic_metadata(record_to_insert):
    '''
    Funcion para insertar metadatos de cierto month y year a metados.extract
    '''
    # Conexion y cursor para query
    connection = psycopg2.connect(user = MY_USER, # Usuario RDS
                                 password = MY_PASS, # password de usuario de RDS
                                 host = MY_HOST,# endpoint
                                 port="5432", # cambiar por el puerto
                                 database=MY_DB) # Nombre de la base de datos
    cursor = connection.cursor()

    # Query para insertar metadatos
    postgres_insert_query = """ INSERT INTO metadatos.semantic (fecha, nombre_task,\
     year, month, usuario, ip_ec2, tamano_zip, nombre_archivo, ruta_s3, \
     task_status) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) """
    cursor.execute(postgres_insert_query, record_to_insert)
    connection .commit()
    cursor.close()
    connection.close()

    return print("Metadadata Insertion Done - PostgreSQL connection is closed")