import sys
import utils
from utils import apikey, rate, payload, regions
import os, sys, signal, threading
import requests
import json
import time
import pandas as pd
import sqlite3
import logging
import psycopg2

lock = threading.Lock()
quitEvent = threading.Event()
quitEvent.clear()
#sanity program ender
def signal_handler(signal, frame):
	global quitEvent
	print("\nprogram exiting gracefully")
	quitEvent.set()
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

database = "data/data.db"

class mainThread(threading.Thread):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, group=None, target=None, name=None, args=(), kwargs=None,daemon=None):
		super().__init__(group=group, target=target, name=name,kwargs=kwargs,daemon=daemon)
		self.region = region
		self.batchsize = batchsize
		self.count = 0
		self.index = 0				
		self.database = database
		self.itemsLeft = -1		
		self.msg = "init"		
		self.tiers = tiers
		self.logger = utils.set_up_logger(name=region,file=f"data/status_{region}.log")	
		self.countQuery = ""
		
	def run(self):
		global quitEvent
		conn = None
		cursor = None
		self.msg = "cnct"
		
		while not quitEvent.is_set() and conn == None:
			try:
				self.logger.info("trying to connect to database")
				conn = psycopg2.connect(dbname=self.database, user=utils.pgUsername, password=utils.pgPassword)
				cursor = conn.cursor()				
			except Exception as err:
				self.logger.error(err)
				self.msg = "NC"
				time.sleep(5)
		self.msg = "SUCC"
		if quitEvent.is_set():
				return;		
		for tier in self.tiers:			
			self.logger.info(f"Starting {tier}")
			self.countQuery = f"SELECT COUNT(*) FROM summoners WHERE region='{self.region}' AND tier='{tier}' AND accountId is NULL"
			query = f"SELECT * FROM summoners WHERE region='{self.region}' "\
					f"AND tier='{tier}' AND accountId is NULL LIMIT {self.batchsize}"		
			cursor.execute(self.countQuery)
			self.itemsLeft, = cursor.fetchone()				
			self.msg = self.itemsLeft
			while self.itemsLeft > 0 and not quitEvent.is_set():
				self.msg = str(self.itemsLeft)			
				records = None
				cols = None
				cursor.execute(query)
				records = cursor.fetchall()
				cols = [d[0] for d in cursor.description]
				
				summonerIdx = cols.index('summonerid')
				for idx, record in enumerate(list(records)):
					summonerId = record[summonerIdx]
					if quitEvent.is_set():
						return;			
					req = f'https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/{summonerId}'
					resp = requests.get(req, headers=payload)
					time.sleep(1/rate)
					accountId = None
					if resp.ok:					
						data = json.loads(resp.content)
						accountId = data['accountId']
					else:					
						self.msg = f"code {resp.status_code}"
						#print("got ", resp.status_code, " for ", record[summonerIdx])
						if resp.status_code == 403:
							self.logger.error(f"Response code {resp.status_code}, need to regenerate API key")
							raise Exception(f"Forbidden, probably need to regenerate API key: {resp.status_code}")				
						elif resp.status_code in [404, 415]:
							accountId = '404'
							self.logger.warning(f"Response code {resp.status_code} for summonerId={summonerId}, setting accountId=0")
						elif resp.status_code == 400:
							self.logger.error(f"Response code {resp.status_code}, bad request for '{req}'")
							accountId = '400'
						elif resp.status_code == 429:
							time.sleep(1/rate)
							self.logger.warning(f"Response code 429, rate limit exceeded, sleeping one cycle")
						else:
							self.logger.info(f"Response code {resp.status_code}, unhandled")
					if accountId!=None:
						records[idx] = (record[0],record[1],record[2],record[3],record[4],accountId)
				self.write_data(records,cols, conn)
			
			if quitEvent.is_set():
				return;
		self.msg = "DONE"
		self.logger.info("Complete")
				
	def write_data(self, records, cols, conn):		
		cursor = conn.cursor()
		summonerIdx = cols.index('summonerid')
		accountIdx = cols.index('accountid')
		regionIdx = cols.index('region')
		tierIdx = cols.index('tier')
		if not quitEvent.is_set():
			#with self.lock:					
			#print("\nwriting records")
			recordCount = 0
			for record in records:
				if record[accountIdx]!=None:
					recordCount+=1							
					updateQuery = f"UPDATE summoners SET accountId='{record[accountIdx]}' WHERE summonerId='{record[summonerIdx]}' and region='{record[regionIdx]}' and tier='{record[tierIdx]}'"																			
					cursor.execute(updateQuery)					
			conn.commit()
			cursor.execute(self.countQuery)
			self.itemsLeft, = cursor.fetchone()
			self.logger.info(f"Comitting {recordCount} new entries to database, {self.itemsLeft} items left")
			self.msg = str(self.itemsLeft)
					

threads = list()

for region in regions:
	#thread = threading.Thread(target=main, args=(region,), daemon = True)	
	thread = mainThread(region, tiers=['PLATINUM','DIAMOND','MASTER','GRANDMASTER','CHALLENGER'],
						batchsize=100)
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
		msg = "\r"
		for thread in threads:
			msg = msg+str(thread.msg)+"\t"
		msg+=str(numRunningThreads)+"\t"
		print(msg,end="")
		if numRunningThreads==0:
			break
		time.sleep(1)
except KeyboardInterrupt:
	quitEvent.set()
	exit(0)
finally:
	quitEvent.set()



'''

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
'''