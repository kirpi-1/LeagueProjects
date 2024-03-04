import sqlite3
con = sqlite3.connect("data/data.db")
cur = con.cursor()
query = '''
CREATE TABLE summoners(
    summonerId string,
    summonerName string,
    tier string,
    rank string,
    region string,
    puuid string)
'''
cur.execute(query)
con.commit()
con.close()
