import requests
import json
import pandas as pd
import time
import sys
import random
import numpy as np
import os
import threading
import signal
from utils import apikey, rate, payload

#sanity program ender
def signal_handler(signal, frame):
	print("\nprogram exiting gracefully")
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


def save_progress(region, rank, division, page):
	with open(f"progress_{region}.txt",'w') as f:
		f.write(f"{rank}\n{division}\n{page}\n")

def load_progress(region):	
	rank = 0
	division = 0
	page= 1
	vals = list()
	if os.path.exists(f"progress_{region}.txt"):
		with open(f"progress_{region}.txt",'r') as f:
			l = f.readline()
			while l!="":
				vals.append(f.readline())
				l = f.readline()
		if len(vals)==3:		
			rank = int(vals[1])
			division = int(vals[2])
			page = int(vals[3])
	
	return rank, division, page

# get list of summoners in regular ranks
apikey = ""
with open("apikey.txt",'r') as f:
	apikey = f.read().strip()
payload = {  
    "X-Riot-Token": apikey
}
rate = 90/120 # the rate at which to make new requests. Inverse for sleep timer


ranks = ['PLATINUM','DIAMOND']#['IRON','BRONZE','SILVER','GOLD','PLATINUM','DIAMOND']
divisions = ['I','II','III','IV']
regions = ['br1','eun1','euw1','jp1','kr','la1','la2','oc1','ru','tr1']#na1 also

def my_thread(region):
	starting_rank_idx, starting_division_idx,starting_page= load_progress(region)
	rank_idx = starting_rank_idx
	filename = f"./data/summoners_{region}.csv"
	if not os.path.exists(filename):
		with open(filename,'w+') as f:
			f.write("tier,rank,summonerId")
	while rank_idx < len(ranks):		
		rank = ranks[rank_idx]		
		out = pd.DataFrame()
		print(rank)
		division_idx = starting_division_idx
		while division_idx < len(divisions):
			division = divisions[division_idx]		
			print('\t',division)
			page = starting_page
			while True:				
				print("\rpage\t",page, end="")
				save_progress(region, rank_idx, division_idx, page)
				req = f'https://{region}.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{rank}/{division}?page={page}'
				resp = requests.get(req,headers=payload)
				time.sleep(1/rate)
				data = json.loads(resp.content)
				if resp.ok and data != []:
					page+=1
					with open(filename,'a+') as f:
						for d in data:
							f.write(",".join((d['tier'],d['rank'],d['summonerId'])))
							f.write("\n")					
				else:
					print(resp)
					if resp.status_code == 429:
						time.sleep(1/rate)
						continue
					if resp.status_code == 403:
						sys.exit()
					else:
						break
					
				#df = pd.DataFrame.from_dict(data)
				#df = df[['leagueId','tier','rank','summonerId','inactive']]				
				#out = pd.concat([out,df])
			starting_page = 1
			print()
			division_idx += 1
		starting_division_idx = 0		
		
		rank_idx += 1	

threads = list()
for region in regions:
	thread = threading.Thread(target=my_thread, args=(region,), daemon = True)	
	threads.append(thread)

for thread in threads:
	thread.start()

for thread in threads:	
	while True:
		thread.join(1)
		if not thread.isAlive():
			break

print('done!')