import sys
from utils import apikey, rate, payload, regions
import os, sys, signal, threading
import requests
import json
import time
import pandas as pd
import sqlite3
import logging

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


class mainThread(threading.Thread):
	def __init__(self, region, lock, batchsize = 100, group=None, target=None, name=None, args=(), kwargs=None,daemon=None):
		super().__init__(group=group, target=target, name=name,kwargs=kwargs,daemon=daemon)
		self.region = region
		self.batchsize = batchsize
		self.count = 0
		self.index = 0		
		self.lock = lock
		self.itemsLeft = -1
		self.msg = ""
		#logging set up
		formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')		
		handler = logging.FileHandler(f"data/status_{region}.log")
		handler.setFormatter(formatter)
		self.logger = logging.getLogger(region)
		self.logger.setLevel(logging.INFO)
		self.logger.addHandler(handler)
		
	def run(self):
		global quitEvent
		countQuery = f"SELECT COUNT(*) FROM summoners WHERE region='{self.region}' AND accountId is NULL"
		query = f"SELECT * FROM summoners WHERE region='{self.region}' AND accountId is NULL LIMIT {self.batchsize}"
		conn = sqlite3.connect(database)
		cursor = conn.cursor()
		with self.lock:			
			self.itemsLeft, = cursor.execute(countQuery).fetchone()
			
		while self.itemsLeft > 0 and not quitEvent.is_set():
			self.msg = str(self.itemsLeft)
			#print(f'\r\t{region}: {itemsLeft} left',end="")			
			records = None
			cols = None
			with self.lock:				
				records = cursor.execute(query).fetchall()
				cols = [d[0] for d in cursor.description]
			summonerIdx = cols.index('summonerId')
			accountIdx = cols.index('accountId')
			regionIdx = cols.index('region')
			tierIdx = cols.index('tier')
			
			for idx, record in enumerate(list(records)):
				
				if quitEvent.is_set():
					break
				summonerId = record[summonerIdx]
				req = f'https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/{summonerId}'
				resp = requests.get(req, headers=payload)
				time.sleep(1/rate)
				accountId = None
				if resp.ok:					
					data = json.loads(resp.content)
					accountId = data['accountId']
					#print("response okay:",accountId)
					
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
					else:
						self.logger.info(f"Response code {resp.status_code}, unhandled")
				if accountId!=None:
					records[idx] = (record[0],record[1],record[2],accountId,record[4])
			
			# write to database
			if not quitEvent.is_set():
				with self.lock:					
					#print("\nwriting records")
					recordCount = 0
					for record in records:
						if record[accountIdx]!=None:
							recordCount+=1
							#print("record=",record)
							updateQuery = f"UPDATE summoners SET accountId='{record[accountIdx]}' WHERE summonerId='{record[summonerIdx]}' and region='{record[regionIdx]}' and tier='{record[tierIdx]}'"												
							#print(updateQuery)	
							cursor.execute(updateQuery)					
					conn.commit()
					self.itemsLeft, = cursor.execute(countQuery).fetchone()
					self.logger.info(f"Comitting {recordCount} new entries to database, {self.itemsLeft} items left")
					




threads = list()

for region in regions:
	#thread = threading.Thread(target=main, args=(region,), daemon = True)	
	thread = mainThread(region, lock,batchsize=100)
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
			msg = msg+str(thread.itemsLeft)+"\t"
		msg+=str(numRunningThreads)+"\t"
		print(msg,end="")
		time.sleep(1)
except KeyboardInterrupt:
	quitEvent.set()
	exit(0)
finally:
	quitEvent.set()

