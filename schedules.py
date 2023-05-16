import schedule
import threading
import time
import subprocess
from reports import send_daily_report, connect_to_database, get_last_trade

def run_daily_report():
    connection = connect_to_database()
    send_daily_report(connection)
 
def run_get_last_trade():
    connection = connect_to_database()
    get_last_trade(connection)

# def run_file2():
#     subprocess.run(["python", "arquivo2.py"])


schedule.every().day.at("18:00").do(run_daily_report)

# Iniciar a função de get_last_trade em uma nova thread
thread = threading.Thread(target=run_get_last_trade)
thread.start()

while True:
    schedule.run_pending()
    time.sleep(1)
