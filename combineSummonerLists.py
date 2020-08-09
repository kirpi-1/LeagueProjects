import sys
from utils import apikey, rate, payload
import os, sys, signal, threading
import requests
import json
import time
import pandas as pd

#sanity program ender
def signal_handler(signal, frame):
	print("\nprogram exiting gracefully")
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

def main(region):
	df1 = pd.read_csv(f"data/summoners_{region}.csv")
	df2 = pd.read_csv(f"data/summoners_expert_{region}.csv")
	out = df1.append(df2, ignore_index=True)
	out.to_csv(f"data/summoners_{region}.csv",index = False)


regions = ['br1','eun1','euw1','jp1','kr','la1','la2','oc1','ru','tr1']#na1 also

threads = list()
for region in regions:
	thread = threading.Thread(target=main, args=(region,), daemon = True)	
	threads.append(thread)

for thread in threads:
	thread.start()

for thread in threads:	
	while True:
		thread.join(1)
		if not thread.isAlive():
			break

print("done!")