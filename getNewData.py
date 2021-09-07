import sys
import utils
from utils import apikey, rate, payload, regions
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



#sanity program ender
quitEvent = threading.Event()
quitEvent.clear()
def signal_handler(signal, frame):
	global quitEvent
	print("\nprogram exiting gracefully")
	quitEvent.set()
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


# Parse arguments

parser = argparse.ArgumentParser()
parser.add_argument('--log', default = "INFO", type=str)
parser.add_argument('-q','--quiet', default=False,action="store_true")
parser.add_argument('-s','--start-date', default="2020-01-01",type=str, help="starting date in YYYY-MM-DD format")
parser.add_argument('-e','--end-date', default="", type=str, help="ending date in YYYY-MM-DD format")
parser.add_argument('-v','--game-version',default=10,type=int,help="season number to look at for eligible summoners")
parser.add_argument('-t','--tiers', default="CGMDP", type=str,help="which tiers to look at, [C]hallenger, [G]randmaster, [M]aster, [D]iamond, [P]latinum")


args = parser.parse_args()
log_level = logging.INFO
if args.log.lower() in utils.LOG_LEVELS:
	log_level = utils.LOG_LEVELS[args.log.lower()]


start_date, end_date = utils.parse_date(args.start_date, args.end_date)

tiers = list()
if 'c' in args.tiers.lower():
	tiers.append('CHALLENGER')
if 'g' in args.tiers.lower():
	tiers.append('GRANDMASTER')
if 'm' in args.tiers.lower():
	tiers.append('MASTER')
if 'd' in args.tiers.lower():
	tiers.append('DIAMOND')
if 'p' in args.tiers.lower():
	tiers.append('PLATINUM')
	


max_num_of_summoners = 1000
LIMIT_RETRIES = 5
games_schema = utils.load_list('games_schema.txt')
games_cols = [c[0] for c in games_schema]
games_types = [c[1] for c in games_schema]

class mainThread(utils.mainThreadProto):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, num_batches = 10, table = 'games', 
				start_date = None, end_date = None, version = 10,
				log_level = logging.INFO, group=None, target=None, name=None, args=(), kwargs=None,daemon=True):
		super().__init__(region=region, tiers=tiers, database=database, batchsize=batchsize, num_batches=num_batches,
						table=table, log_level=log_level, group=group, target=target, name=name,
						kwargs=kwargs,daemon=daemon)
		self.start_date = start_date
		self.end_date = end_date
		self.version = version
						
	def run(self):		
		global quitEvent
		self.msg = "cnct"
		self.logger.info("Getting game data")
		self.logger.debug("Debugging is ON")		
		for tier in self.tiers:
			self.current_tier = tier
			if self.conn == None:
				self.initConn()
				self.msg = "SUCC"
			self.logger.info(f"Starting {tier}")
			
			# get list of summoners who have played during season 10
			summonerids = self.getSummoners()				
			for idx, summonerId in enumerate(summonerids):
				self.logger.info(f"working on {idx} of {len(summonerids)}, summoner {summonerId}")
				self.msg = len(summonerids)-idx
				# get the info on hand for this summonerid				
				query = f"SELECT * FROM summoners WHERE summonerid='{summonerId}' and region='{self.region}'"\
				
				# this gets checked below
				#AND (last_accessed is NULL or EXTRACT(EPOCH FROM last_accessed) > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) - 604800)"
				err = self.execute(query)
				if err:
					continue
				result = self.cursor.fetchone()
				cols = self.getColumns()
				puuidIdx = cols.index('puuid')
				accountidIdx = cols.index('accountid')
				last_accessedIdx = cols.index('last_accessed')
				# if missing account id or puuid, we should fetch it
				if result[puuidIdx]==None or result[accountidIdx]==None:
					self.logger.info("missing puuid or accountid, updating...")
					self.updateSummonerInfo(summonerId)
					query = f"SELECT * FROM summoners WHERE summonerid='{summonerId}' and region='{self.region}'"
					err = self.execute(query)
					if err:
						continue
					result = self.cursor.fetchone()
				# construct dict for the getMatchList functions
				s = dict()
				s['summonerid'] = summonerId
				s['accountid'] = result[accountidIdx]
				s['last_accessed'] = result[last_accessedIdx]
				# get the matchlist for this summoner if the last time i looked at them was more than a week ago
				if s['last_accessed'] is None or time.time() - s['last_accessed'].timestamp() > 7*24*60*60: # 1 week					
					df = self.getMatchList(s)
					# write it to the database
					if len(df)>0:
						df.to_sql("games", self.engine, index=False, if_exists='append', method=utils.writeGameInit)
						
					now = time.time()
					query = f"UPDATE summoners SET last_accessed=to_timestamp({now}) WHERE summonerid='{summonerId}' and region='{self.region}'"
					self.execute(query, True)
				
				elif time.time() - s['last_accessed'].timestamp() < 7*24*60*60:
					self.logger.info(f"Last looked at summonerid({s['summonerid']}) less than a week ago ({s['last_accessed']})")
			
			
		
		self.close()
		self.msg = "DONE"
		self.logger.info("Complete")
	
	def getSummoners(self):
		version = self.version
		self.logger.info(f"Getting summoners who participated in games with version={version}")
		# get list of games where game version is 10.*			
		query = f"SELECT p1_summonerid, p2_summonerid, p3_summonerid, p4_summonerid, p5_summonerid, p6_summonerid,"\
				f"p7_summonerid, p8_summonerid, p9_summonerid, p10_summonerid FROM games WHERE region='{self.region}' AND tier='{self.current_tier}' and gameversion LIKE '{version}.%'"
		results = None
		cols = None
		err = self.execute(query, False)
		if err:
			self.logger.error("something went wrong getting summoners")
			exit(0)
		results = self.cursor.fetchall()
		cols = [d[0] for d in self.cursor.description]
		
		# get every summonerid that participated in those games
		summonerids = list()
		for r in results:
			for p in r:
				summonerids.append(p)
		summonerids = list(set(summonerids))		
		
		# get list of summoners that aren't in the database
		query = f"SELECT * from summoners where region='{self.region}'"
		results = None
		cols = None
		if self.execute(query, False) == False:				
			results = self.cursor.fetchall()
			cols = self.getColumns()
		df = pd.DataFrame(results, columns=cols)
		new_summoners = set(summonerids).difference(set(df['summonerid']))					
		self.logger.debug(f"found {len(new_summoners)} out of {len(summonerids)} new summoners")
		if len(new_summoners)>0:
			self.writeSummoners(new_summoners)
		return summonerids
	
	def writeSummoners(self, new_summoners):				
		self.logger.info(f"Writing {len(new_summoners)} new summoners to database")
		tmp = list()
		for s in new_summoners:
			tmp.append(f"('{s}','{self.region}','{self.current_tier}')")
		insertQuery = "INSERT INTO summoners (summonerid, region, tier) VALUES " + ", ".join(tmp)
		self.logger.debug(f"writing: {insertQuery}")
		err = self.execute(insertQuery, True)
		if err:
			return err

	def updateSummonerInfo(self, summonerId):
		self.logger.debug(f"updating summoner {summonerId}")
		r = dict()								
		r['puuid'] = None					
		r['summonerid'] = summonerId
		r['accountid'] = None
		r['summonername'] = None
		req = f'https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/{summonerId}'					
		resp = self.request(req)		
		if resp.ok:					
			data = json.loads(resp.content)						
			r['puuid'] = data['puuid']						
			r['summonerid'] = data['id']
			r['accountid'] = data['accountId']
			r['summonername'] = data['name']
			
		else:					
			if self.handleHTTPError(resp)==404:
				r['puuid'] = 404
				r['summonerid'] = summonerId
				self.logger.error(f"404 error when updating summoner {summonerId}")
		
		# update the database
		updateQuery = f"UPDATE summoners SET puuid='{r['puuid']}', accountid='{r['accountid']}', summonername='{r['summonername']}' WHERE summonerid='{r['summonerid']}' and region='{self.region}'"
		self.execute(updateQuery, True)
		
	def getMatchList(self, summonerInfo):
	# summonerInfo is a dict with at least the following keys:	
	# 		summonerid, accountid, last_accessed
	# returns a matchlist for this summoner as a pandas.DataFrame
		self.logger.debug(f"Getting matchlist for summonerId={summonerInfo['summonerid']}, accountId={summonerInfo['accountid']}")
		# start date and end date
		start_timestamp = None
		end_timestamp = time.time()
		# if don't have a set start date
		if self.start_date is None:					
			# Use the default start date only if this summoner hasn't been accessed before
			if summonerInfo['last_accessed'] is None:
				start_timestamp = time.mktime(utils.DEFAULT_START_DATE)			
			else: # otherwise only get games after the last time we accessed
				start_timestamp = summonerInfo['last_accessed'].timestamp()		
			
		else: # use the set start date
			start_timestamp = time.mktime(self.start_date)
		
		# if an end_date is set, then use that if it is after the start date
		if self.end_date is not None:			
			end_timestamp = time.mktime(self.end_date)
			if end_timestamp < start_timestamp:
				end_timestamp = time.time()
		
		df = pd.DataFrame()
		beginIndex = 0
		while True:
			resp, data = self.requestMatchList(summonerInfo['accountid'], beginIndex=beginIndex)
			if not resp.ok:			
				self.logger.error(f"Error when requesting matches for summonerid='{summonerInfo['summonerid']}'")
				break;
			if data==None or data['matches']==[]:
				self.logger.debug(f"reached end of matches for {summonerInfo['summonerid']}")
				break;
			df = df.append(data['matches'], ignore_index=True)
			
			#timestamp from Riot is in milliseconds, so need to divide it by 1000 for seconds
			earliest_timestamp = df.iloc[-1]['timestamp']/1000
			# if the earliest timestamp from a game is greater than our starting timestamp,
			# increment beginIndex so we can get even earlier timestamps
			if earliest_timestamp > start_timestamp: 
				beginIndex+=100
			# otherwise, if we find a game that is earlier than our start date, then we can stop requesting matches
			else:			
				break;
		
		# set all the column names to lowercase, then extract only the relevant columns
		# then select only the games that happened after the starting timestamp and before the end timestamp
		if len(df)>0:
			df.columns= df.columns.str.strip().str.lower()
			df = df[['gameid','queue','timestamp','platformid']]
			df = df[df['timestamp']/1000>=start_timestamp]
			df = df[df['timestamp']/1000<=end_timestamp]
			df['tier'] = self.current_tier
			df['region'] = self.region
		self.logger.debug(f"got {len(df)} matches")
		return df
	
	def requestMatchList(self, accountId, beginIndex = None, endIndex = None):
		# Helper function for getMatchList to perform the actual request
		self.logger.debug(f"Requesting matchlist for accountId={accountId}, beginIndex={beginIndex}")
		extras = list()
		extras.append("queue=420")
		if beginIndex != None:
			extras.append(f"beginIndex={beginIndex}")
		if endIndex != None:
			if endIndex > beginIndex and endIndex - beginIndex <= 100:
				extras.append(f"endIndex={endIndex}")
			else:
				self.logger.warning(f"endIndex must be greater than beginIndex and less than or equal to beginIndex+100, got beginIndex = {beginIndex} and endIndex = {endIndex}")				
		req = f"https://{self.region}.api.riotgames.com/lol/match/v4/matchlists/by-account/{accountId}?"+"&".join(extras)		
		resp = self.request(req)
		self.logger.debug(f"got {resp.status_code} for request: {req}")
		if resp.ok:
			data = json.loads(resp.content)
			return resp, data
			
		else:
			self.logger.debug("Error when requesting {req}")
			self.handleHTTPError(resp)
			return resp, None		

	

	
	def getColumns(self):
		return [d[0] for d in self.cursor.description]		
	
		
	def getItemsLeft(self, tier):
		self.logger.debug("Getting count of items that are left")
		countQuery = f"SELECT COUNT(*) FROM {self.table} WHERE region='{self.region}' AND tier='{tier}' AND queueid IS NULL"
		self.execute(countQuery, False)
		self.itemsLeft, = self.cursor.fetchone()	
	
		
	def write_data(self, gameList):			
		global games_cols
		self.logger.info("Writing data...")
		updateCount = 0
		
		if len(gameList)>0:
			for idx,g in enumerate(gameList):
				self.logger.debug(f"Doing game {idx} of {len(gameList)}, id={g['gameId']},queueId={g['queueId']}")				
				cols = g.keys()				
				# build column=value pairs
				tmp = list()
				for key in cols:
					if key != "gameId" and key.lower() in games_cols:
						if isinstance(g[key], str):
							tmp.append(f"{key} = '{g[key]}'")
						elif g[key] == None:
							tmp.append(f"{key} = NULL")
						else:
							tmp.append(f"{key} = {g[key]}")
				
				updateQuery = f"UPDATE {self.table} SET " + ", ".join(tmp) + f" WHERE gameid={g['gameId']}"
				self.logger.debug(updateQuery)
				try:
					self.execute(updateQuery, commit=False)
				except:
					self.logger.error(f"Something might be wrong with gameid {g['gameId']} usinq query of len {len(updateQuery)}")
				else:
					updateCount += 1
		retry_counter = 0
		while retry_counter < LIMIT_RETRIES:
			try:				
				self.logger.debug("Committing update")
				self.conn.commit()
			except psycopg2.OperationalError as e:				
				retry_counter+=1
				self.logger.error(f"{str(e)}, connection probably timed out, retrying {retry_counter}")
				self.reset()
			else:
				break
				
		self.logger.info(f"Updating data for {updateCount} of {len(gameList)}  games")	
		

threads = list()
#regions = ['na1']
for region in regions:
	#thread = threading.Thread(target=main, args=(region,), daemon = True)	
	thread = mainThread(region, 
		tiers=tiers,batchsize=100, num_batches=100, table='games', log_level=log_level,
		version=args.game_version, start_date=start_date, end_date=end_date)
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
	
	
	
	