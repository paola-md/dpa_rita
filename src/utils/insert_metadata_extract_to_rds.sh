#!/bin/bash
# Progama que hace la insersion de datos desde data.csv hacia raw.rita
# Nota: necesita que el archio credentials_psql.txt este presente, el cual
# contiene los datos que permite la conexion a psql con el RDS
# user = MY_USER
# password = MY_PASS
# db_name = MY_DB
# host = MY_HOST

. credentials_psql.txt

PGPASSWORD=$password psql -U $user -h $host -d $db_name -c "\COPY raw.rita FROM 'data.csv'  WITH CSV HEADER;"

echo "Data insertion on raw.rita completed"
