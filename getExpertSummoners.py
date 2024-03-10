import sys
from utils import apikey, rate, payload, regions
import utils
import os, sys, signal, threading
import requests
import json
import time
import pandas as pd
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

class mainThread(utils.mainThreadProto):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, num_batches = 10, table = 'games',
				log_level = logging.INFO, group=None, target=None, name=None, args=(), kwargs=None,daemon=True):
		super().__init__(region=region, tiers=tiers, database=database, batchsize=batchsize, num_batches=num_batches,
						table=table, log_level=log_level, group=group, target=target, name=name,
						kwargs=kwargs,daemon=daemon)
		self.data = []
		self.data_old = None

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
				data['tier'] = names[idx]
				self.write_data(data)
			else: # resp was not okay
				return self.handleHTTPError(resp)

	def write_data(self,data):
		if self.conn == None:
			self.initConn()
		self.logger.info("Writing data")
		now = datetime.datetime.now()
		count = 0
		if not quitEvent.is_set():
			for d in data['entries']:
				self.logger.debug(d)
				rank = self.getRank(d['rank'])
				query = "INSERT INTO summoners(summonerId, summonerName, leaguePoints, tier, rank, region, last_updated) "\
						f"VALUES ('{d['summonerId']}','{d['summonerName']}','{d['leaguePoints']}', '{data['tier']}', '{rank}','{self.region}', {now})"
				#print("\n",query,"\n")
				err = self.execute(query, commit=False)
				if err is not None:
					if err is psycopg.errors.UniqueViolation:
						continue
				else:
					self.commit()
					count+=1;
		self.logger.info(f"Committed {count} entries to database")


threads = list()
for region in regions:
	thread = mainThread(region, lock, database = 'league')
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
