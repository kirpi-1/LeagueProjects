import sys
import utils
from utils import apikey, rate, payload, regions, quitEvent
import os, sys, signal, threading
import requests
import json
import time
import pandas as pd
import numpy as np
import sqlite3
import logging
import psycopg2
import argparse
import logging

# given a database that is populated with games, get new data based on the summoners who have been playing

parser = argparse.ArgumentParser()
parser.add_argument('--log', default = "INFO", type=str)
parser.add_argument('-q','--quiet', default=False,action="store_true") 
args = parser.parse_args()
log_level = logging.INFO
if args.log.lower() in utils.LOG_LEVELS:
	log_level = utils.LOG_LEVELS[args.log.lower()]

def signal_handler(signal, frame):
	global quitEvent
	print("\nprogram exiting gracefully")
	quitEvent.set()
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

max_num_of_summoners = 1000

games_schema = utils.load_list('games_schema.txt')
games_cols = [c[0] for c in games_schema]
games_types = [c[1] for c in games_schema]
class mainThread(utils.mainThreadProto):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, num_batches = 10, table = 'games', 
				log_level = logging.INFO, group=None, target=None, name=None, args=(), kwargs=None,daemon=True):
		super().__init__(region=region, tiers=tiers, database=database, batchsize=batchsize, num_batches=num_batches,
						table=table, log_level=log_level, group=group, target=target, name=name,
						kwargs=kwargs,daemon=daemon)

	def run(self):		
		global quitEvent
		self.msg = "cnct"
		self.logger.info("Getting game data")
		self.logger.debug("Debugging is ON")		
		for tier in self.tiers:			
			if self.conn == None:
				self.initConn()
				self.msg = "SUCC"
			self.logger.info(f"Starting {tier}")			
			
			query = f"SELECT * FROM summoners WHERE region='{self.region}' "\
					f"AND tier='{tier}' AND puuid is NULL LIMIT {self.batchsize}"		
			self.countQuery = f"SELECT COUNT(*) FROM summoners WHERE region='{self.region}' AND tier='{tier}' AND puuid IS NULL"
			
			self.getItemsLeft(tier)			
			count = 0
			while self.itemsLeft > 0 and count < self.num_batches:				
				self.msg = str(self.num_batches-count)
				records = None
				cols = None
				self.logger.info(f"Starting batch {count+1} of {self.num_batches}")
				# get {batchsize} {tier} summoners in {region} that haven't been accessed yet
				if self.execute(query, False):
					self.logger.error("Error getting next batch of data")
					time.sleep(5)
					continue
				
				records = self.cursor.fetchall()
				cols = self.getColumns()
				
				#close the connection so it doesn't idle out
				self.close()
				
				puuidIdx = cols.index('puuid')
				summoneridIdx = cols.index('summonerid')
				newRecords = list()
				for idx, record in enumerate(list(records)):
					r = dict()					
					summonerId = record[summoneridIdx]
					req = f'https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/{summonerId}'					
					resp = self.request(req)
					time.sleep(1/rate)
					
					if resp.ok:					
						data = json.loads(resp.content)						
						r['puuid'] = data['puuid']						
						r['summonerid'] = data['id']
						r['accountid'] = data['accountId']
						newRecords.append(r)
						
					else:					
						if self.handleHTTPError(resp)==404:
							r['puuid'] = 404
							r['summonerid'] = summonerId
							newRecords.append(g)
				# reconnect to the server
				self.initConn()
				self.write_data(newRecords)							
				count += 1	
				self.getItemsLeft(tier)
				# end while
		self.close()
		self.msg = "DONE"
		self.logger.info("Complete")
		
	def write_data(self, newRecords):			
		global games_cols
		self.logger.info("Writing data...")
		updateCount = 0
		
		if len(newRecords)>0:
			for idx,r in enumerate(newRecords):
				self.logger.debug(f"Doing game {idx} of {len(newRecords)}, id={r['summonerid']},puuid={r['puuid']}")
				
				updateQuery = f"UPDATE summoners SET puuid='{r['puuid']}', accountid='{r['accountid']}' WHERE summonerid='{r['summonerid']}'"
				self.logger.debug(updateQuery)
				err = self.execute(updateQuery, commit=False)			
				if err:
					self.logger.error("something went wrong writing")
				else:
					updateCount += 1
		
		self.commit()
		self.logger.info(f"Updating data for {updateCount} of {len(newRecords)}  games")		


threads = list()
#regions = ['na1']
for region in regions:
	#thread = threading.Thread(target=main, args=(region,), daemon = True)	
	thread = mainThread(region, tiers=['CHALLENGER', 'GRANDMASTER', 'MASTER', 'DIAMOND','PLATINUM'],
						batchsize=100, num_batches=100, table='games', log_level=log_level)
	threads.append(thread)

for thread in threads:
	thread.start()

msg = ""
for region in regions:
	msg = msg+region+"\t"
msg+="running threads"
if not args.quiet:
	print(msg)
try:
	while not quitEvent.is_set():
		numRunningThreads = 0
		for thread in threads:
			if thread.is_alive():
				numRunningThreads+=1
			else:
				thread.msg = "EXIT"
		
		
		msg = "\r"
		for thread in threads:
			msg = msg+str(thread.msg)+"\t"
		msg+=str(numRunningThreads)+"\t"
		
		if not args.quiet:
			print("\r" + " "*120, end="")
			print(msg,end="")
		if numRunningThreads==0:
			break
		time.sleep(1)
except KeyboardInterrupt:
	for thread in threads:
		if thread.is_alive():
			thread
	quitEvent.set()
	
	exit(0)
finally:
	quitEvent.set()
	
	
	
	