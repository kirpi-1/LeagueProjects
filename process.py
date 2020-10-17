import utils
import psycopg2
import pandas as pd
import numpy as np



conn = psycopg2.connect(dbname='league', user=utils.pgUsername, password=utils.pgPassword)
cursor = conn.cursor()

cursor.execute("SELECT * FROM games WHERE gameversion LIKE '10.20%'")
records = cursor.fetchall()

description = cursor.description()

cols = [c.name for c in description]

df = pd.DataFrame(records)
df.columns = cols

c = list()
for p in range(10):
	c.append(f"p{p+1}_totaldamagedealttochampions")


df['meandamagedealttochampions'] = df[c].mean()
df['stddamagedealttochampions'] = df[c].std()