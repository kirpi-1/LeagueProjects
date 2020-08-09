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
	# get list of summoners in higher ranks
	queues = ['RANKED_SOLO_5x5']
	queue = 'RANKED_SOLO_5x5'
	baseurl = f'https://{region}.api.riotgames.com'
	leagues = [f'/lol/league/v4/challengerleagues/by-queue/{queue}',
			   f'/lol/league/v4/grandmasterleagues/by-queue/{queue}',
			   f'/lol/league/v4/masterleagues/by-queue/{queue}'
			  ]
	names = ['CHALLENGER', 'GRANDMASTER', 'MASTER']
	filename = f'./data/summoners_expert_{region}.csv'
	l = 0
	out = pd.DataFrame()
	for idx, l in enumerate(leagues):
		url = baseurl+l    
		resp = requests.get(url, headers=payload)    
		time.sleep(1/rate)
		if resp.ok:
			data = json.loads(resp.content)			
			df = pd.DataFrame.from_dict(data)
			df['rank']=[a['rank'] for a in df['entries']]
			df['summonerId']=[a['summonerId'] for a in df['entries']]			
			df = df[['tier','rank','summonerId']]
			df['accountId'] = None
			out = out.append(df, ignore_index=True)
		else:
			print(url, resp)
	out.to_csv(filename, index = False)


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


