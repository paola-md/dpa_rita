#!/bin/bash
# Progama que hace la insersion de datos desde data.csv hacia semantic.rita
# Nota: necesita que el archio credentials_psql.txt este presente, el cual
# contiene los datos que permite la conexion a psql con el RDS
# user = MY_USER
# password = MY_PASS
# db_name = MY_DB
# host = MY_HOST

. credentials_psql.txt

# Correcion para evitar errores de importacion con psql
cat semantic.csv | sed 's/\"\"/\N/g' > semantic.csv

# importa datos hacia psql
PGPASSWORD=$password psql -U $user -h $host -d $db_name -c "\COPY semantic.rita FROM 'data.csv'  WITH CSV HEADER;"

echo "Data insertion on semantic.rita completed"
