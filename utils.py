apikey = ""
with open("apikey.txt",'r') as f:
	apikey = f.read().strip()
payload = {  
    "X-Riot-Token": apikey
}
rate = 90/120 # the rate at which to make new requests. Inverse for sleep timer
regions = ['br1','eun1','euw1','jp1','kr','la1','la2','oc1','ru','tr1']#na1 also