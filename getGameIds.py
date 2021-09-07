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
import argparse

#sanity program ender
quitEvent = threading.Event()
quitEvent.clear()
#sanity program ender
def signal_handler(signal, frame):
	global quitEvent
	print("\nprogram exiting gracefully")
	quitEvent.set()
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

max_num_of_summoners = 1000
LIMIT_RETRIES = 5

parser = argparse.ArgumentParser()
parser.add_argument('--log', default = "INFO", type=str)
parser.add_argument('-q','--quiet', default=False,action="store_true")
parser.add_argument('-t','--tiers', default="CGMDP", type=str,help="which tiers to look at,\n"\
	"[C]hallenger, [G]randmaster, [M]aster, [D]iamond, [P]latinum, G[O]ld, [S]ilver, [B]ronze, [I]ron")
parser.add_argument('-s','--batchsize', default=100, type=int, help="batch size")
parser.add_argument('-n','--numbatches',default=100,type=int, help="number of batches")
args = parser.parse_args()
log_level = logging.INFO
if args.log.lower() in utils.LOG_LEVELS:
	log_level = utils.LOG_LEVELS[args.log.lower()]

print("log level:",log_level)
class mainThread(threading.Thread):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, num_batches = 100, group=None, target=None, name=None, args=(), kwargs=None,daemon=None,log_level=logging.INFO):
		super().__init__(group=group, target=target, name=name,kwargs=kwargs,daemon=daemon)
		self.region = region
		self.batchsize = batchsize
		self.num_batches = num_batches
		self.count = 0
		self.index = 0		
		self.database = database
		self.itemsLeft = -1		
		self.msg = "init"		
		self.tiers = tiers
		self.logger = utils.set_up_logger(name=region,file=f"data/status_{region}.log",level=log_level)
		self.countQuery = ""
		self.conn = None
		self.cursor = None
	
	def __del__(self):
		self.logger.info("Ending")
		self.close()
				
	def run(self):
		global quitEvent		
		self.msg = "cnct"
		self.logger.info("Getting Game IDs")		
		
		for tier in self.tiers:
			if self.conn == None:
				self.initConn()
				self.msg = "SUCC"
			self.current_tier = tier
			self.logger.info(f"Starting {tier}")
			
			if self.conn == None:
				self.initConn()
				self.msg = "SUCC"			
			timestamp_info = "to_timestamp('10/14/2020 01:00', 'MM/DD/YYYY HH24:MI')"
			self.countQuery = f"SELECT COUNT(*) FROM summoners WHERE region='{self.region}' AND tier='{tier}' AND (last_accessed <= {timestamp_info} OR last_accessed is NULL) ;"
			query = f"SELECT * FROM summoners WHERE region='{self.region}' "\
					f"AND tier='{tier}' AND (last_accessed <= {timestamp_info} OR last_accessed is NULL) LIMIT {self.batchsize}"		
			
			self.getItemsLeft(tier)
			self.msg = self.itemsLeft
			count = 0
			while self.itemsLeft > 0 and count < self.num_batches:				
				self.msg = str(self.num_batches-count)
				self.logger.info(f"{self.itemsLeft} items left")
				records = None
				cols = None
				# get {batchsize} {tier} summoners in {region} that haven't been accessed yet
				self.execute(query)				
				records = self.cursor.fetchall()				
				cols = [d[0] for d in self.cursor.description]								
				accountIdx = cols.index('accountid')
				
				self.close()
				
				gameList = list()
				accountList = list()
				for idx, record in enumerate(list(records)):
					accountId = record[accountIdx]
					
					req = f'https://{self.region}.api.riotgames.com/lol/match/v4/matchlists/by-account/{accountId}'#?beginIndex=100'
					
					resp = self.request(req)
					time.sleep(1/rate)
					
					if resp.ok:					
						data = json.loads(resp.content)						
						# get only soloqueue summoner's rift
						gameIds = [g['gameId'] for g in data['matches'] if g['queue']==420]
						#gameCreation = [g['timestamp'] for g in data['matches'] if g['queue']==420]
						accountList.append(accountId)
						gameList = gameList + gameIds
					else:					
						self.msg = f"c{resp.status_code}"						
						if resp.status_code == 403:
							self.logger.error(f"Response code {resp.status_code}, need to regenerate API key")
							raise Exception(f"Forbidden, probably need to regenerate API key: {resp.status_code}")				
						elif resp.status_code in [404, 415]:							
							self.logger.warning(f"Response code {resp.status_code} for accountId={accountId}, setting accountId=0")
							accountList.append(accountId)
						elif resp.status_code == 400:
							self.logger.error(f"Response code {resp.status_code}, bad request for '{req}'")
							accountId = '400'
						elif resp.status_code == 429:
							time.sleep(1/rate)
							self.logger.warning(f"Response code 429, rate limit exceeded, sleeping one cycle")
						else:
							self.logger.info(f"Response code {resp.status_code}, unhandled")
				
				self.initConn()
				err = self.write_data(gameList, accountList)			
				if err:
					self.msg = "ERROR"
					return;
				count += 1
				
				self.getItemsLeft(tier)				
				# end while

		self.msg = "DONE"
		self.logger.info("Complete")
	
	def getItemsLeft(self, tier):		
		self.execute(self.countQuery, False)
		self.itemsLeft, = self.cursor.fetchone()	
	
	def request(self, req):
		retry_counter=0
		while retry_counter < LIMIT_RETRIES:
			try:
				resp = requests.get(req, headers=payload)
			except Exception as e:
				self.logger.error(f"{str(e)} when requesting: {req}")
				if retry_counter < LIMIT_RETRIES:					
					retry_counter+=1
					self.logger.error(f"Retrying {retry_counter}...")
					self.msg = f"eREQ{retry_counter}"
					time.sleep(5)					
				else:
					self.logger.error(f"Exceeded max retries ({LIMIT_RETRIES})")
					return None
				
			else:
				return resp
	
	def execute(self, query, commit = True):
		retry_counter = 0
		while retry_counter < LIMIT_RETRIES:
			try:
				if self.conn == None:
					self.initConn()
				if self.cursor == None:
					self.getCursor()
				self.logger.debug(query)
				self.cursor.execute(query)
				
			except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
				if retry_counter >= LIMIT_RETRIES:
					self.logger.error("Reached max retries with query: " + str(error))
					self.conn.rollback()					
					self.msg = "ERR"
					return True
				else:
					retry_counter += 1
					self.logger.error(f"{str(error).strip()}, retrying {retry_counter}")
					time.sleep(1)
					self.reset()
					
			except (Exception, psycopg2.Error) as error:
				self.logger.error(f"{str(error).strip()}")
				self.conn.rollback()
				self.msg = "ERR"
				return True
			else:
				if commit:
					self.conn.commit()
				return False		
				
	def reset(self):
		self.close()
		self.connect()
		self.getCursor()
		
	def connect(self):
		conn = None
		retry_counter = 0
		while not quitEvent.is_set() and conn == None and retry_counter < LIMIT_RETRIES:
			try:
				self.logger.info("Trying to connect to database...")				
				conn = psycopg2.connect(dbname=self.database, user=utils.pgUsername, password=utils.pgPassword,host=utils.pgHost)
			except Exception as err:
				retry_counter += 1
				self.logger.error(str(err))
				self.msg = "NC"
				time.sleep(5)
			else:
				self.conn = conn
				self.logger.info(f"Successfully connected to database, PID = 	{conn.info.backend_pid}")				
				#self.msg = "SUCC"
				
	def close(self):
		if self.cursor:
			self.cursor.close()
		if self.conn:
			self.conn.close()
		self.logger.info("PostgreSQL connection closed")
		self.conn = None
		self.cursor = None
	
	def getCursor(self):
		if self.conn == None:
			self.logger.error("cursor does not exist, cannot fetch")
		else:
			self.cursor = self.conn.cursor()
		
	def initConn(self):
		self.connect()
		self.getCursor()
		
	def write_data(self, gameList, accountList):				
		self.logger.info("Writing data...")		
		accountList = list(set(accountList)) # remove duplicates
		err = False
		if not quitEvent.is_set():					
			if len(gameList)>0:
				tmp = list()
				for g in gameList:
					tmp.append(f"({g},'{self.region}','{self.current_tier}')")			
				insertQuery = "INSERT INTO games (gameid, region, tier) VALUES " + ", ".join(tmp) + " ON CONFLICT ON CONSTRAINT games_gameid_key DO NOTHING;"
				
				err = self.execute(insertQuery)
				if err:
					return err
				
			updateQuery = f"UPDATE summoners SET last_accessed=CURRENT_TIMESTAMP WHERE "
			if len(accountList)>0:
				tmp = list()
				for a in accountList:
					tmp.append(f"accountid='{a}'")
				updateQuery += " OR ".join(tmp) + ";"				
				err = self.execute(updateQuery)
				if err:
					return err				
			self.logger.info(f"Comitting {len(gameList)} new entries to games, updating {len(accountList)} summoners")
		return False


threads = list()
for region in regions:
	#thread = threading.Thread(target=main, args=(region,), daemon = True)	
	thread = mainThread(region, tiers=tiers,
						batchsize=args.batchsize, num_batches=args.numbatches, log_level=log_level)
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
		print("\r"+" "*80,end="")
		print(msg,end="")
		if numRunningThreads==0:
			break
		time.sleep(10)
except KeyboardInterrupt:
	quitEvent.set()
	exit(0)
finally:
	quitEvent.set()
	
	
	
	
	
	
	
	
'''
def main(region):
	
	filename = f'./data/summoners_{region}.csv'	
	source = pd.read_csv(filename)
	totalItems = len(source)    
	count = 0
	games = list()
	rows = list(range(len(source)))
	num_plat = 0
	if len(rows)>max_num_of_summoners:
		random.shuffle(rows)
		rows = rows[0:max_num_of_summoners]

	tiers = ['PLATINUM','DIAMOND','MASTER','GRANDMASTER','CHALLENGER']	
	for tier in tiers:
		outfile = f"data/gameIds_{region}_{tier}.csv"
		with open(outfile,'w+') as f:
			f.write("gameId,tier,region\n")
		df = source[source['tier']==tier]
		df = df.sample(frac=1)
		index = 0
		while index < len(df) and index < max_num_of_summoners:
			print(f"\r{index+1} of {len(df)}", end="")
			if not pd.isnull(df['accountId'][index:index+1]:
				accountId = df['accountId'][index:index+1].values[0]
				resp = requests.get(f'https://{region}.api.riotgames.com/lol/match/v4/matchlists/by-account/{accountId}', headers=payload)
				time.sleep(1/rate)
				if resp.ok:
					data = json.loads(resp.content)
					gameIds = [a['gameId'] for a in data['matches'] if a['queue']==420]# or a['queue']==440]
					with open(outfile,'a+') as f:
						for g in gameIds:
							f.write(f"{g},{tier},{region}"
						
					index += 1
				else:
					display(resp)
					if resp.status_code>=400 and resp.status_code < 420:
						raise Exception(f"bad response from server: {resp.status_code}")
	print()
'''