import os, sys, signal, threading, time, logging, argparse
import requests
import json
import pandas as pd
import numpy as np
import sqlite3
import psycopg2
from sqlalchemy import create_engine


quitEvent = threading.Event()
quitEvent.clear()


apikey = ""
with open("apikey.txt",'r') as f:
	apikey = f.read().strip()
payload = {  
    "X-Riot-Token": apikey
}
rate = 90/120 # the rate at which to make new requests. Inverse for sleep timer
regions = ['na1','br1','eun1','euw1','jp1','kr','la1','la2','oc1','ru','tr1']

pgUsername = ""
pgPassword = ""

jan12020 = 1577865600 #epoch time of jan 1st, 2020


LOG_LEVELS = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}


with open('pguser.txt','r') as f:
	pgUsername = f.readline().strip()
	pgPassword = f.readline().strip()
	pgHost	   = f.readline().strip()

def write_list(l, filename):
	#filename = add_txt_ext(filename)
	if os.path.dirname(filename)!='':
		os.makedirs(os.path.dirname(filename), exist_ok=True)
	with open(filename,'w',encoding='utf-8',errors='ignore') as f:
		for item in l:
			f.write(str(item)+'\n')

def load_list(filename):
	out = []
	with open(filename,'r', encoding='utf-8', errors ='ignore') as f:
		l = f.readline()
		while l!="":
			l = l.strip()
			if l=="":
				continue
			if l[0] == '(':
				l = l[1:]
			if l[-1] == ')':
				l=l[:-1]
			l = l.replace("'","")			
			l = l.split(',')
			out.append(l)
			l = f.readline()
	return out

def set_up_logger(name="default", file="data/default.log", logFormat = '%(asctime)s %(levelname)s %(message)s', level=logging.INFO):
	#logging set up
	formatter = logging.Formatter(logFormat)		
	handler = logging.FileHandler(file,encoding='utf-8')
	handler.setFormatter(formatter)
	logger = logging.getLogger(name)
	logger.setLevel(level)
	logger.addHandler(handler)
	return logger
	
LIMIT_RETRIES = 5	
class mainThreadProto(threading.Thread):
	def __init__(self, region, tiers, database = 'league', batchsize = 100, num_batches = 10, table = 'games', log_level = logging.INFO,
				group=None, target=None, name=None, args=(), kwargs=None,daemon=True):
		super().__init__(group=group, target=target, name=name,kwargs=kwargs,daemon=daemon)		
		self.region = region
		self.batchsize = batchsize
		self.num_batches = num_batches
		self.table = table
		self.count = 0
		self.index = 0		
		self.database = database
		self.itemsLeft = -1		
		self.msg = "init"		
		self.tiers = tiers		
		self.logger = set_up_logger(name=region,file=f"data/status_{region}.log", level=log_level)
		self.countQuery = ""
		self.conn = None
		self.cursor = None
		self.engine = None
	
	def __del__(self):
		self.logger.info("Ending")
		self.close()		
		
	def getColumns(self):
		return [d[0] for d in self.cursor.description]		
	
	def handleHTTPError(self, resp):
		self.msg = f"c{resp.status_code}"			
		if resp.status_code == 403:
			self.logger.error(f"Response code {resp.status_code}, need to regenerate API key")
			raise Exception(f"Forbidden, probably need to regenerate API key: {resp.status_code}")				
		elif resp.status_code in [404, 415]:							
			self.logger.warning(f"Response code {resp.status_code}")			
			return 404			
		elif resp.status_code == 400:
			self.logger.error(f"Response code {resp.status_code}, bad request for '{req}'")
			accountId = '400'
			return 400
		elif resp.status_code == 429:
			time.sleep(1/rate)
			self.logger.warning(f"Response code 429, rate limit exceeded, sleeping one cycle")
		else:
			self.logger.info(f"Response code {resp.status_code}, unhandled")
		
		return None
		
	def getItemsLeft(self, tier):
		self.logger.debug("Getting count of items that are left")		
		self.execute(self.countQuery, False)
		self.itemsLeft, = self.cursor.fetchone()	
	
	def request(self, req, retry_limit = LIMIT_RETRIES):
		retry_counter=0
		old_msg = self.msg
		while retry_counter < retry_limit:
			try:
				resp = requests.get(req, headers=payload)
				time.sleep(1/rate)
			except Exception as e:
				self.logger.error(f"{str(e)} when requesting: {req}")
				if retry_counter < retry_limit:					
					retry_counter+=1
					self.logger.error(f"Retrying {retry_counter}...")
					self.msg = f"eREQ{retry_counter}"
					time.sleep(5)										
				else:
					self.logger.error(f"Exceeded max retries ({retry_limit})")
					return None
				
			else:
				self.msg = old_msg
				
				return resp
	
	def execute(self, query, commit = True, retry_limit = LIMIT_RETRIES):
		retry_counter = 0
		old_msg = self.msg
		while retry_counter < retry_limit:
			try:
				if self.conn == None:
					self.initConn()
				if self.cursor == None:
					self.getCursor()
				self.cursor.execute(query)				
			except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
				if retry_counter >= retry_limit:
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
				self.msg = old_msg
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
				conn = psycopg2.connect(dbname=self.database, user=pgUsername, password=pgPassword, host=pgHost)
				engine = create_engine(f"postgresql+psycopg2://{pgUsername}:{pgPassword}@/{self.database}?host={pgHost}")					
			except Exception as err:
				retry_counter += 1
				self.logger.error(str(err))
				self.msg = "NC"
				time.sleep(5)
			else:
				self.conn = conn
				self.engine = engine
				self.logger.info(f"Successfully connected to database, PID = 	{conn.info.backend_pid}")
				self.msg = "SUCC"
	
	def commit(self, retry_limit = LIMIT_RETRIES):
		retry_counter = 0
		while retry_counter < LIMIT_RETRIES:
			try:				
				self.logger.debug("Committing update")
				self.conn.commit()
			except psycopg2.OperationalError as e:				
				if retry_counter < retry_limit:
					retry_counter+=1
					self.logger.error(f"{str(e)}, connection probably timed out, retrying {retry_counter}")
					self.reset()
				else:
					self.logger.error("Max retry limit reached for committing to database")
					return True
			else:				
				break
		return False
	
	def close(self):
		if self.cursor:
			self.cursor.close()
		if self.conn:
			self.conn.close()
		self.logger.info("PostgreSQL connection closed")
		self.conn = None
		self.cursor = None
	
	def getCursor(self):
		self.cursor = self.conn.cursor()
		
	def initConn(self):
		self.connect()
		self.getCursor()
		
def writeGameInit(pd_table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """        
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:        
        query = f"INSERT INTO {pd_table.name} (gameid, gamecreation, queueid, platformid, region, tier) VALUES "
        vals = list()
        for data in data_iter:
            gameid = data[keys.index('gameid')]
            gamecreation = data[keys.index('timestamp')]
            queueid = data[keys.index('queue')]
            platformid = data[keys.index('platformid')]
            region = data[keys.index('region')]
            tier = data[keys.index('tier')]
            vals.append(f"({gameid}, {gamecreation}, {queueid}, '{platformid}', '{region}', '{tier}')")
        query += ", ".join(vals) +" ON CONFLICT ON CONSTRAINT games_gameid_key DO NOTHING;"        
        cur.execute(query)