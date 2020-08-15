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

max_num_of_summoners = 1000

def main(region):
	
	filename = f'./data/summoners_{region}.csv'	
	source = pd.read_csv(filename)
	totalItems = len(source)    
	count = 0
	games = list()
	rows = list(range(len(source)))
	num_plat = 0
	if len(rows)>max_num_of_summoners:
		random.shuffle(rows)
		rows = rows[0:max_num_of_summoners]

	tiers = ['PLATINUM','DIAMOND','MASTER','GRANDMASTER','CHALLENGER']	
	for tier in tiers:
		outfile = f"data/gameIds_{region}_{tier}.csv"
		with open(outfile,'w+') as f:
			f.write("gameId,tier,region\n")
		df = source[source['tier']==tier]
		df = df.sample(frac=1)
		index = 0
		while index < len(df) and index < max_num_of_summoners:
			print(f"\r{index+1} of {len(df)}", end="")
			if not pd.isnull(df['accountId'][index:index+1]:
				accountId = df['accountId'][index:index+1].values[0]
				resp = requests.get(f'https://{region}.api.riotgames.com/lol/match/v4/matchlists/by-account/{accountId}', headers=payload)
				time.sleep(1/rate)
				if resp.ok:
					data = json.loads(resp.content)
					gameIds = [a['gameId'] for a in data['matches'] if a['queue']==420]# or a['queue']==440]
					with open(outfile,'a+') as f:
						for g in gameIds:
							f.write(f"{g},{tier},{region}"
						
					index += 1
				else:
					display(resp)
					if resp.status_code>=400 and resp.status_code < 420:
						raise Exception(f"bad response from server: {resp.status_code}")
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
