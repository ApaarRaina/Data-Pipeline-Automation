import pyodbc
import pandas as pd
import os
from datetime import datetime
from plyer import notification
import json
import pyarrow
from fastavro import writer, parse_schema
import datetime

with open("config.json", "r") as f:
    config = json.load(f)


server=config['server']
database=config['database']
username=config['username']
password=config['password']
source_db=config['source_db']
target_db=config['target_db']
processed_db=config['processed_db']


#----------------------------- Exporting sql data to CSV,Paraquet and Avro-------------------
connection = pyodbc.connect(
    "DRIVER={MySQL ODBC 9.3 Unicode Driver};"
    f"SERVER={server};"
    f"DATABASE={database};"     
    f"UID={username};"
    f"PWD={password};"
)


os.makedirs('exports',exist_ok=True)

cursor = connection.cursor()

# Get all table names
cursor.execute("show tables")
tables = [row[0] for row in cursor.fetchall()]

def infer_avro_schema(df, name='AutoSchema'):
    fields = []
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            avro_type = 'int'
        elif pd.api.types.is_float_dtype(dtype):
            avro_type = 'float'
        elif pd.api.types.is_bool_dtype(dtype):
            avro_type = 'boolean'
        else:
            avro_type = 'string'
        fields.append({'name': col, 'type': ['null', avro_type]})
    return {
        'doc': 'Auto-generated schema',
        'name': name,
        'namespace': 'example',
        'type': 'record',
        'fields': fields
    }

for table in tables:
    query= f"select* from {table}"
    df = pd.read_sql(sql=query, con=connection)

    df.to_csv(f"exports/{table}.csv", index=False)
    df.to_parquet(f"exports/{table}.parquet", engine='pyarrow', index=False)

    df['join_date'] = df['join_date'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    records = df.to_dict(orient='records')
    schema = parse_schema(infer_avro_schema(df))
    with open(f"exports/{table}.avro", "wb") as out:
        writer(out, schema, records)



#--------------------------Transfering tables from one database to another---------------------

server_conn = pyodbc.connect(
    f"DRIVER={{MySQL ODBC 9.3 Unicode Driver}};"
    f"SERVER={server};UID={username};PWD={password};"
)
server_cursor = server_conn.cursor()


server_cursor.execute(f"SHOW DATABASES LIKE '{target_db}'")
if not server_cursor.fetchone():
    print(f"Creating target database `{target_db}`...")
    server_cursor.execute(f"create database `{target_db}`")
    server_conn.commit()
    print(f"Database `{target_db}` created.")


source_conn = pyodbc.connect(
    f"DRIVER={{MySQL ODBC 9.3 Unicode Driver}};"
    f"SERVER={server};DATABASE={source_db};UID={username};PWD={password};"
)
target_conn = pyodbc.connect(
    f"DRIVER={{MySQL ODBC 9.3 Unicode Driver}};"
    f"SERVER={server};DATABASE={target_db};UID={username};PWD={password};"
)

source_cursor=source_conn.cursor()
target_cursor=target_conn.cursor()

source_cursor.execute("show tables")
tables=[row[0] for row in source_cursor.fetchall()]

for table in tables:

    source_cursor.execute(f"show create table {table}")
    create_stmt=source_cursor.fetchone()[1]
    create_stmt = create_stmt.replace(f"CREATE TABLE `{table}`", f"CREATE TABLE `{target_db}`.`{table}`")

    target_cursor.execute(f"show tables like '{table}'")

    if not target_cursor.fetchone():
        target_cursor.execute(create_stmt)


    df=pd.read_sql(f"select * from {table}",source_conn)

    cols = ",".join([f"`{col}`" for col in df.columns])
    placeholders = ",".join(["?"] * len(df.columns))
    insert_sql = f"INSERT IGNORE INTO `{table}` ({cols}) VALUES ({placeholders})"
    target_cursor.executemany(insert_sql, list(df.itertuples(index=False)))

    target_conn.commit()


#-------------------------Copying data according to business requirements---------------------
server_cursor.execute(f"show databases like '{processed_db}'")

if not server_cursor.fetchone():
    server_cursor.execute(f"CREATE DATABASE {processed_db}")
    server_conn.commit()
    print(f"Database `{processed_db}` created.")


processed_conn = pyodbc.connect(
    f"DRIVER={{MySQL ODBC 9.3 Unicode Driver}};"
    f"SERVER={server};DATABASE={processed_db};UID={username};PWD={password};"
)

processed_cursor=processed_conn.cursor()

for table in tables:
    source_cursor.execute(f"show create table {table}")
    create_stmt=source_cursor.fetchone()[1]
    create_stmt = create_stmt.replace(f"CREATE TABLE '{table}'", f"CREATE TABLE `{processed_db}`.`{table}`")

    processed_cursor.execute(f"show tables like '{table}'")

    if not processed_cursor.fetchone():
        processed_cursor.execute(create_stmt)

    df=pd.read_sql(f"select* from {table} where {table}.subscription_status='active'",source_conn)
    cols = ",".join([f"`{col}`" for col in df.columns])
    placeholders = ",".join(["?"] * len(df.columns))
    insert_sql = f"INSERT IGNORE INTO {table} ({cols}) VALUES ({placeholders})"
    processed_cursor.executemany(insert_sql, list(df.itertuples(index=False)))

    processed_conn.commit()


