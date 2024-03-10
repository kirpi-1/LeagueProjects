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
import datetime
import psycopg

lock = threading.Lock()
quitEvent = threading.Event()
quitEvent.clear()

#sanity program ender
def signal_handler(signal, frame):
	print("\nprogram exiting gracefully")
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


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

ranks = ['IRON','BRONZE','SILVER','GOLD','PLATINUM','EMERALD','DIAMOND']
divisions = ['I','II','III','IV']
regions = utils.regions

class mainThread(utils.mainThreadProto):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, num_batches = 10, table = 'games',
				log_level = logging.INFO, group=None, target=None, name=None, args=(), kwargs=None,daemon=True):
		super().__init__(region=region, tiers=tiers, database=database, batchsize=batchsize, num_batches=num_batches,
						table=table, log_level=log_level, group=group, target=target, name=name,
						kwargs=kwargs,daemon=daemon)
		self.data = []
		self.data_old = None

	def save_progress(self, region, rank_idx, division_idx, page):
		self.logger.info(f"saving progress to {rank_idx}, {division_idx}, {page}")
		with open(f"data/progress_{region}.txt",'w') as f:
			f.write(f"{rank_idx}\n{division_idx}\n{page}\n")

	def getSummonerNames(self, rank, division, page):
		req = f'https://{self.region}.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{rank}/{division}?page={page}'
		self.logger.debug(req)
		resp = requests.get(req,headers=payload)
		time.sleep(1/rate)
		d = json.loads(resp.content)
		if resp.ok and d != []: #everything went correctly
			for entry in d:
				tmp = {'summonerId':entry['summonerId'],
					   'summonerName':entry['summonerName'],
					   'tier':entry['tier'],
					   'rank':entry['rank'],
					   'region':self.region}
				self.data.append(tmp)
			self.logger.debug(f"Data is len=({len(self.data)})")
			return 0
		elif resp.ok and d==[]: #reached the end of pages
			return 1
		else: # resp was not okay
			return self.handleHTTPError(resp)


	def run(self):
		global quitEvent
		region = self.region
		starting_rank_idx, starting_division_idx,starting_page = load_progress(region)
		self.logger.info(f"starting at {starting_rank_idx}, {starting_division_idx}, {starting_page}")
		rank_idx = starting_rank_idx
		data = []
		# iterate through ranks
		while rank_idx < len(ranks):
			rank = ranks[rank_idx]
			out = pd.DataFrame()
			# Iterate through divisions
			division_idx = starting_division_idx
			while division_idx < len(divisions):
				division = divisions[division_idx]
				# iterate through pages
				page = starting_page
				while not quitEvent.is_set():
					self.msg = f"{rank[0]}{division_idx+1}{page}"
					self.save_progress(region, rank_idx, division_idx, page)
					res = self.getSummonerNames(rank, division, page)
					if res == 1:
						break
					if len(self.data)>=self.batchsize:
						self.write_data()
					page+=1
				# Write any remaining entries
				if len(self.data)>0:
					self.write_data()
				starting_page = 1
				division_idx += 1
			starting_division_idx = 0
			rank_idx += 1
		self.close()
		self.msg = "DONE"
		self.logger.info(f"{region} complete")

	def write_data(self):
		if self.conn == None:
			self.initConn()
		self.logger.info("Writing data")
		now = datetime.datetime.now()
		count = 0
		if not quitEvent.is_set():
			base_query = "INSERT INTO summoners(summonerId, summonerName, tier, rank, region, last_updated) VALUES "
			for d in self.data:
				rank = self.getRank(d['rank'])
				query = base_query + f"('{d['summonerId']}','{d['summonerName']}','{d['tier']}','{rank}','{self.region}', {now})"
				err = self.execute(query, commit=False)
				if err is not None:
					if err is psycopg.errors.UniqueViolation:
						continue
				else:
					self.commit()
					count+=1

		self.logger.info(f"Committed {count} entries to database")
		self.data_old = pd.DataFrame(self.data)
		self.data = []


threads = list()
for region in regions:
	thread = mainThread(region, lock, database = 'league', log_level=logging.DEBUG)
	threads.append(thread)

for thread in threads:
	thread.start()

padding = 10
msg = ""
for region in regions:
	msg = f"{msg}"+f"{region:^{padding}}"
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
			msg = f"{msg}"+f"{str(thread.msg):^{padding}}"
		msg+=f'{str(numRunningThreads):^{padding}}'+"\t"
		print(msg,end="")
		time.sleep(1)
except KeyboardInterrupt:
	quitEvent.set()
	exit(0)
finally:
	quitEvent.set()
