import sys
from utils import apikey, rate, payload, regions
import utils
import os, sys, signal, threading
import requests
import json
import time
import pandas as pd
import sqlite3


lock = threading.Lock()
quitEvent = threading.Event()
quitEvent.clear()

#sanity program ender
def signal_handler(signal, frame):
	print("\nprogram exiting gracefully")
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

with open('pguser.txt','r') as f:
	pgUsername = f.readline().strip()
	pgPassword = f.readline().strip()
	pgHost	   = f.readline().strip()

class mainThread(utils.mainThreadProto):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, num_batches = 10, table = 'games',
				log_level = logging.INFO, group=None, target=None, name=None, daemon=True, args=(), kwargs=None):
		super().__init__(region=region, tiers=tiers, database=database, batchsize=batchsize, num_batches=num_batches,
						table=table, log_level=log_level, group=group, target=target, name=name,daemon=daemon,						
						kwargs=kwargs)


	def construct_endpoints(self):
		queues = ['RANKED_SOLO_5x5']
		queue = 'RANKED_SOLO_5x5'
		leagues = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'EMERALD', 'DIAMOND']
		divisions = ['I','II','III','IV','V']
		endpoints = {}
		for league in leagues:
		    for division in divisions:
		        endpoints[league+" "+division] = f'/lol/league/v4/entries/{queue}/{league}/{division}'
		# add '?page={page}'
	def run(self):
		baseurl = f'https://{self.region}.api.riotgames.com'
		all_data = list()
		for key in endpoints.keys():
		    self.logger.info(f"Starting {key}")
		    l = endpoints[key]
		    quit = False
		    page = 1
		    while not quit:
		        url = baseurl+l+f"?page={page}"
		        resp = requests.get(url, headers=payload)
				time.sleep(1/rate)
		        if resp.ok:
		            data = json.loads(resp.content)
		            if len(data)>0:
		                quit = False
		                print(f"Finished page {page:03d}", end='\r')
		                all_data += data
		                page+=1
		            else:
		                quit = True
				else:
					self.msg = f"code {resp.status_code}"
					if resp.status_code == 403:
						self.logger.error(f"Response code {resp.status_code}, need to regenerate API key")
						raise Exception(f"Forbidden, probably need to regenerate API key: {resp.status_code}")
					elif resp.status_code == 429:
						time.sleep(1/rate)
					elif resp.status_code in [404, 415]:
						accountId = '404'
						self.logger.warning(f"Response code {resp.status_code} for summonerId={summonerId}, setting accountId=0")
					elif resp.status_code == 400:
						self.logger.error(f"Response code {resp.status_code}, bad request for '{req}'")
						accountId = '400'
					else:
						self.logger.info(f"Response code {resp.status_code}, unhandled")
					self.msg = f'E{resp.status_code}''
		df = pd.DataFrame(all_data)

		for idx, l in enumerate(leagues):
			url = baseurl+l
			resp = requests.get(url, headers=payload)
			time.sleep(1/rate)
			if resp.ok:
				data = json.loads(resp.content)
				self.write_data(data)



threads = list()
for region in regions:
	thread = mainThread(region, lock, database = 'league', pginfo = {'username':pgUsername, 'password':pgPassword,'host':pgHost})
	threads.append(thread)

for thread in threads:
	thread.start()

msg = ""

for region in regions:
	msg = msg+region+"\t"
msg+="running threads"
print(msg)
try:
	while True:
		numRunningThreads = 0
		for thread in threads:
			if thread.is_alive():
				numRunningThreads+=1
		if numRunningThreads == 0:
			print("\n\ndone!")
			break
		msg = "\r"
		for thread in threads:
			msg = msg+str(thread.msg)+"\t"
		msg+=str(numRunningThreads)+"\t"
		print(msg,end="")
		time.sleep(1)
except KeyboardInterrupt:
	quitEvent.set()
	exit(0)
finally:
	quitEvent.set()
