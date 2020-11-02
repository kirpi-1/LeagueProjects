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

class mainThread(threading.Thread):
	def __init__(self, region, lock, batchsize = 100, database = 'data/data.db',group=None, target=None, name=None, args=(), kwargs=None,daemon=None):
		super().__init__(group=group, target=target, name=name,kwargs=kwargs,daemon=daemon)
		self.region = region
		self.lock = lock
		self.batchsize = batchsize		
		self.logger = utils.set_up_logger(name=region,file=f"data/status_{region}.log")		
		self.msg = ""
		self.database = database

	def run(self):		
		region = self.region
		queues = ['RANKED_SOLO_5x5']
		queue = 'RANKED_SOLO_5x5'
		baseurl = f'https://{region}.api.riotgames.com'
		leagues = [f'/lol/league/v4/challengerleagues/by-queue/{queue}',
				   f'/lol/league/v4/grandmasterleagues/by-queue/{queue}',
				   f'/lol/league/v4/masterleagues/by-queue/{queue}'
				  ]
		names = ['CHALLENGER', 'GRANDMASTER', 'MASTER']
		
								
		for idx, l in enumerate(leagues):
			url = baseurl+l    
			resp = requests.get(url, headers=payload)    
			time.sleep(1/rate)
			if resp.ok:
				data = json.loads(resp.content)
				self.write_data(data)				
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
				
		
		
	def write_data(self,data):
		#print(data['entries'][0])
		conn = sqlite3.connect(self.database)
		cursor = conn.cursor()
		tier = data['tier']
		if not quitEvent.is_set():
			with self.lock:
				for d in data['entries']:
					self.logger.info(d)
					query = "INSERT INTO summoners(summonerId, summonerName, tier, rank, region) "\
							f"VALUES ('{d['summonerId']}','{d['summonerName']}','{tier}','{d['rank']}','{self.region}')"
							
					#print("\n",query,"\n")
					cursor.execute(query)
				self.logger.info(f"Committing {len(data)} entries to database")
				conn.commit()
		conn.close()



threads = list()
for region in regions:
	thread = mainThread(region, lock, database = 'data/data2.db')
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

