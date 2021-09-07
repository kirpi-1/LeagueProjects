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

#sanity program ender
quitEvent = threading.Event()
quitEvent.clear()
#sanity program ender

parser = argparse.ArgumentParser()
parser.add_argument('--log', default = "INFO", type=str)
parser.add_argument('-q','--quiet', default=False,action="store_true") 
parser.add_argument('-b','--batches',default=10, type=int, action="store")
parser.add_argument('-s','--start-date', default="2020-01-01",type=str, help="starting date in YYYY-MM-DD format")
parser.add_argument('-e','--end-date', default="2021-01-01", type=str, help="ending date in YYYY-MM-DD format")
parser.add_argument('-v','--game-version',default=10,type=int,help="season number to look at for eligible summoners")

args = parser.parse_args()
log_level = logging.INFO
if args.log.lower() in utils.LOG_LEVELS:
	log_level = utils.LOG_LEVELS[args.log.lower()]
num_batches = args.batches
start_date, end_date = utils.parse_date(args.start_date, args.end_date)

def signal_handler(signal, frame):
	global quitEvent
	print("\nprogram exiting gracefully")
	quitEvent.set()
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

max_num_of_summoners = 1000
LIMIT_RETRIES = 5
games_schema = utils.load_list('games_schema.txt')
games_cols = [c[0] for c in games_schema]
games_types = [c[1] for c in games_schema]
class mainThread(utils.mainThreadProto):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, num_batches = 10, table = 'games', 
				start_date = None, end_date = None,	log_level = logging.INFO, group=None, target=None, name=None, 
				args=(), kwargs=None,daemon=True):
		super().__init__(region=region, tiers=tiers, database=database, batchsize=batchsize, num_batches=num_batches,
						table=table, log_level=log_level, group=group, target=target, name=name,
						kwargs=kwargs,daemon=daemon)
		self.start_date = start_date
		self.end_date = end_date
		
		
	def getStartEndTimestamps(self):
		start_timestamp = time.mktime(utils.DEFAULT_START_DATE)
		end_timestamp = time.time()
		
		if self.start_date is not None:	
			start_timestamp = time.mktime(self.start_date)
			
		# if an end_date is set, then use that if it is after the start date
		if self.end_date is not None:			
			end_timestamp = time.mktime(self.end_date)
			if end_timestamp < start_timestamp:
				end_timestamp = time.time()
		return start_timestamp, end_timestamp
	
	def run(self):		
		global quitEvent
		self.msg = "cnct"
		self.logger.info("Getting game data")
		self.logger.debug("Debugging is ON")		
		self.logger.debug(f"start date: {self.start_date}, end date: {self.end_date}")
		start_timestamp, end_timestamp = self.getStartEndTimestamps()
		
		for tier in self.tiers:			
			if self.conn == None:
				self.initConn()
				self.msg = "SUCC"
			self.logger.info(f"Starting {tier}")
			
			query = f"SELECT * FROM {self.table} WHERE region='{self.region}' "\
					f"AND tier='{tier}' AND gameversion is NULL and queueid!=404 "\
					f"AND gamecreation < {end_timestamp*1000} AND gamecreation > {start_timestamp*1000} "\
					f"ORDER BY gamecreation DESC LIMIT {self.batchsize}"	
			self.logger.debug("Using query: " + query)
			self.countQuery = f"SELECT COUNT(*) FROM {self.table} WHERE region='{self.region}' AND tier='{tier}' AND gameversion IS NULL AND gamecreation >{start_timestamp*1000} and gamecreation < {end_timestamp*1000} and queueid != 404"
			self.getItemsLeft()
			count = 0
			self.logger.info(f"{self.itemsLeft} records left")
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
				cols = [d[0] for d in self.cursor.description]
				
				#close the connection so it doesn't idle out
				self.close()
				
				gameIdIdx = cols.index('gameid')
				gameList = list()
				for idx, record in enumerate(list(records)):
					gameId = record[gameIdIdx]					
					
					req = f'https://{self.region}.api.riotgames.com/lol/match/v4/matches/{gameId}'					
					resp = self.request(req)					
					
					if resp.ok:					
						data = json.loads(resp.content)
						g = self.getGameInfo(data)
						gameList.append(g)						
						
					else:					
						self.msg = f"c{resp.status_code}"						
						if resp.status_code == 403:
							self.logger.error(f"Response code {resp.status_code}, need to regenerate API key")
							raise Exception(f"Forbidden, probably need to regenerate API key: {resp.status_code}")				
						elif resp.status_code in [404, 415]:							
							self.logger.warning(f"Response code {resp.status_code} for gameId={gameId}, setting queueId to 404")
							gameData = dict()
							gameData['region'] = self.region
							gameData['tier'] = tier
							gameData['gameId'] = gameId
							gameData['queueId'] = 404
							gameList.append(gameData)
							
						elif resp.status_code == 400:
							self.logger.error(f"Response code {resp.status_code}, bad request for '{req}'")
							accountId = '400'
						elif resp.status_code == 429:
							time.sleep(1/rate)
							self.logger.warning(f"Response code 429, rate limit exceeded, sleeping one cycle")
						else:
							self.logger.info(f"Response code {resp.status_code}, unhandled")				
				# reconnect to the server
				self.initConn()
				self.write_data(gameList)							
				count += 1	
				self.getItemsLeft()
				self.logger.info(f"{self.itemsLeft} records left")
				# end while
		self.close()
		self.msg = "DONE"		
		self.logger.info("Complete")
	
	
	
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
		
	def getGameInfo(self, data):
		blue = 0
		red = 1
		redTeamId = 200
		blueTeamId = 100
		out = dict()
		if data['teams'][0]['teamId']==redTeamId: # blue team is 100, red team is 200
			blue = 1
			red = 0
		keys = ['gameId', 'platformId', 'gameCreation', 'gameDuration', 'queueId', 'mapId', 'seasonId', 'gameVersion', 'gameMode',
				'gameType']
		for key in keys:
			out[key] = data[key]
		keys = ['win', 'firstBlood', 'firstTower', 'firstInhibitor','firstBaron','firstDragon','firstRiftHerald']
		for key in keys:
			out[key] = 0
			if key in data['teams'][blue] and data['teams'][blue][key] != False and data['teams'][blue][key] != 'Fail':
				out[key] = 100
			elif key in data['teams'][red] and data['teams'][red][key] != False and data['teams'][red][key] != 'Fail':
				out[key] = 200
		
		teams=['blue','red']
		keys = ['towerKills','inhibitorKills','baronKills','dragonKills','riftHeraldKills']		
		for idx,team in enumerate(teams):
			for key in keys:
				if key in data['teams'][idx]:
					out[f"{team}_{key}"] = data['teams'][idx][key]
		'''
		out['blue_towerKills'] = data['teams'][blue]['towerKills']
		out['red_towerKills'] = data['teams'][red]['towerKills']
		out['blue_inhibitorKills'] = data['teams'][blue]['inhibitorKills']
		out['red_inhibitorKills'] = data['teams'][red]['inhibitorKills']
		out['blue_baronKills'] = data['teams'][blue]['baronKills']
		out['red_baronKills'] = data['teams'][red]['baronKills']
		out['blue_dragonKills'] = data['teams'][blue]['dragonKills']
		out['red_dragonKills'] = data['teams'][red]['dragonKills']
		out['blue_riftHeraldKills'] = data['teams'][blue]['riftHeraldKills']
		out['red_riftHeraldKills'] = data['teams'][red]['riftHeraldKills']    
		'''
		#participants = data['participants']
		#blueTeam = [a for a in participants if a['teamId']==blueTeamId]
		#redTeam = [a for a in participants if a['teamId']==redTeamId]
		for idx, participant in enumerate(data['participants']):
			p = self.reduce_participant_fields(participant)
			if 'player' in data['participantIdentities'][idx].keys() and 'summonerId' in data['participantIdentities'][idx]['player'].keys():
				out[f"p{participant['participantId']}_summonerId"] = data['participantIdentities'][idx]['player']['summonerId']
			else:
				out[f"p{participant['participantId']}_summonerId"] = '0'        
			for key in p.keys():            
				out[f"p{participant['participantId']}_{key}"] = p[key]        
		return out
	
	def reduce_participant_fields(self, participant):
		keys = ['championId','spell1Id','spell2Id','teamId']
		out = dict()
		for key in keys:
			if key in participant.keys():
				out[key] = participant[key]
			else:
				out[key] = None
		# from stats:
		keys = ['item0','item1','item2','item3','item4','item5','kills','deaths','assists',
		'totalDamageDealtToChampions','magicDamageDealtToChampions', 'physicalDamageDealtToChampions', 'trueDamageDealtToChampions',
		'totalHeal','damageSelfMitigated','damageDealtToObjectives','damageDealtToTurrets',
		'visionScore','timeCCingOthers','totalDamageTaken','magicalDamageTaken','physicalDamageTaken','trueDamageTaken',
		'goldEarned','goldSpent','turretKills','inhibitorKills','totalMinionsKilled',
		'champLevel','visionWardsBoughtInGame','sightWardsBoughtInGame','wardsPlaced','wardsKilled',
		'perk0','perk1','perk2','perk3','perk4','perk5','perkPrimaryStyle','perkSubStyle']
		for key in keys:
			if key in participant['stats'].keys():
				out[key] = participant['stats'][key]
			else:
				out[key] = None
		# from timeline
		keys=['role','lane']
		for key in keys:
			if key in participant['timeline'].keys():
				out[key] = participant['timeline'][key]    
			else:
				out[key] = None    
		return out

start_time = time.time()
end_time = time.time()
threads = list()
#regions = ['na1']
for region in regions:
	#thread = threading.Thread(target=main, args=(region,), daemon = True)	
	thread = mainThread(region, tiers=['CHALLENGER', 'GRANDMASTER', 'MASTER', 'DIAMOND','PLATINUM'],
						batchsize=100, num_batches=num_batches, table='games', log_level=log_level,
						start_date=start_date, end_date=end_date)
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
			end_time = time.time()
			print(f"\nfinished in {end_time-start_time} seconds")
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
	
	
	
	