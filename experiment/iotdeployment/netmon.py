import psutil
import time
import sys
import json

measure = 0
while(True):
    measure = psutil.net_io_counters(pernic=True)["wlan0"].bytes_sent
    time.sleep(1)
    tx = (psutil.net_io_counters(pernic=True)["wlan0"].bytes_sent - measure) / 1024.0
    cpu = psutil.cpu_percent()
    mem = (psutil.virtual_memory().total - psutil.virtual_memory().available) / 1024 / 1024
    output = {"bw": tx, "cpu": cpu, "mem": mem}
    print json.dumps(output)
    sys.stdout.flush()
