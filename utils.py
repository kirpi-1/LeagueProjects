import logging
import os

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