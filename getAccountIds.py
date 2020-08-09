import sys
from utils import apikey, rate, payload, regions
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
	checkpoint = 1350
	filename = f'./data/summoners_{region}.csv'
	df = pd.read_csv(filename)    
	totalItems = len(df)
	print(filename)
	count = 0
	index = 0
	while index< len(df):
		print(f'\r\t{index} of {totalItems}',end="")
		summonerId = df['summonerId'][index]
		if pd.isnull(df['accountId'][index]):
			req = f'https://{region}.api.riotgames.com/lol/summoner/v4/summoners/{summonerId}'
			resp = requests.get(req, headers=payload)
			time.sleep(1/rate)
			if resp.ok:
				data = json.loads(resp.content)
				df['accountId'][index] = data['accountId']
				count += 1
				index += 1
			else:
				if resp.status_code == 403:
					raise Exception(f"bad response from server: {resp.status_code}")				
		else:
			index += 1
		if count>=checkpoint:
			print(f'\ncheckpoint')
			df.to_csv(filename,index=False)
			count = 0
			print()
		
	df.to_csv(filename,index=False)
	print()




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
