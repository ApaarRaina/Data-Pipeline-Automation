import os
import time
from datetime import datetime
import pyodbc
import json

with open("config.json", "r") as f:
    config = json.load(f)

server = config['server']
database = config['database']
username = config['username']
password = config['password']

conn = pyodbc.connect(
    f"DRIVER={{MySQL ODBC 9.3 Unicode Driver}};"
    f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
)

cursor = conn.cursor()

def get_state():
    state = {}
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            count = cursor.fetchone()[0]
        except:
            count = None
        state[table] = count
    return state

previous_state = get_state()
print("Initial state:", previous_state)

while True:
    current_state = get_state()
    print("Current state:", current_state)

    changed = False
    for table in current_state:
        prev_count = previous_state.get(table)
        curr_count = current_state[table]
        if curr_count != prev_count:
            print(f"[{datetime.now()}] Detected change in `{table}`: {prev_count} → {curr_count}")
            changed = True
            break

    if changed:
        print("Running automation.py...")
        os.system("python automation.py")

    previous_state = current_state
