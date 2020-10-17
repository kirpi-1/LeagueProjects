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
import utils
from utils import apikey, rate, payload
import sqlite3
import logging


lock = threading.Lock()
quitEvent = threading.Event()
quitEvent.clear()

#sanity program ender
def signal_handler(signal, frame):
	print("\nprogram exiting gracefully")
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


def save_progress(region, rank, division, page):
	with open(f"data/progress_{region}.txt",'w') as f:
		f.write(f"{rank}\n{division}\n{page}\n")

def load_progress(region):	
	filename = f"data/progress_{region}.txt"
	rank = 0
	division = 0
	page= 1
	vals = list()
	if os.path.exists(filename):
		with open(filename,'r',encoding='utf-8') as f:
			l = f.readline()
			while l!="":				
				vals.append(l)
				l = f.readline()
		if len(vals)==3:		
			rank = int(vals[0])
			division = int(vals[1])
			page = int(vals[2])		
	
	return rank, division, page

# get list of summoners in regular ranks

ranks = ['IRON','BRONZE','SILVER','GOLD','PLATINUM','DIAMOND']
divisions = ['I','II','III','IV']
regions = ['na1','br1','eun1','euw1','jp1','kr','la1','la2','oc1','ru','tr1']#na1 also

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
		global quitEvent
		region = self.region		
		starting_rank_idx, starting_division_idx,starting_page = load_progress(region)
		self.logger.info(f"starting at {starting_rank_idx}, {starting_division_idx}, {starting_page}")
		rank_idx = starting_rank_idx
		while rank_idx < len(ranks):		
			rank = ranks[rank_idx]		
			out = pd.DataFrame()			
			division_idx = starting_division_idx
			while division_idx < len(divisions):
				division = divisions[division_idx]		
				page = starting_page
				while not quitEvent.is_set():				
					self.msg = page					
					save_progress(region, rank_idx, division_idx, page)
					self.logger.info(f"saving progress to {rank_idx}, {division_idx}, {page}")
					req = f'https://{self.region}.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{rank}/{division}?page={page}'
					resp = requests.get(req,headers=payload)
					time.sleep(1/rate)
					data = json.loads(resp.content)
					if resp.ok and data != []: #everything went correctly
						page+=1
						self.write_data(data)	
					elif resp.ok and data==[]: #reached the end of pages
						break
					else: # resp was not okay						
						if resp.status_code == 429:
							self.logger.warning("429 Rate limite exceeded, waiting another cycle")
							time.sleep(1/rate)
							self.msg = ("ERR429")
							continue
						if resp.status_code == 403:
							self.logger.error("403 FORBIDDEN, Something wrong with api key")
							self.msg = 'ERROR403'
							return;
						else:
							break
				starting_page = 1
				division_idx += 1
			starting_division_idx = 0			
			rank_idx += 1
		self.msg = "done"
	
	def write_data(self, data):
		conn = sqlite3.connect(self.database)
		cursor = conn.cursor()
		if not quitEvent.is_set():
			with self.lock:
				for d in data:								
					query = "INSERT INTO summoners(summonerId, summonerName, tier, rank, region) "\
							f"VALUES ('{d['summonerId']}','{d['summonerName']}','{d['tier']}','{d['rank']}','{self.region}')"									
					cursor.execute(query)
				self.logger.info(f"Committing {len(data)} entries to database")
				conn.commit()

		
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
