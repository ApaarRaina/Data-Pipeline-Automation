import os
import time
from datetime import datetime

INTERVAL_SECONDS = 86400

while True:
    print(f"Running automation.py at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    os.system("python automation.py")

    print(f"Waiting {INTERVAL_SECONDS} seconds before next run...\n")
    time.sleep(INTERVAL_SECONDS)
