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
    puuid string,
    accountId string)
'''
cur.execute(query)
con.commit()
con.close()


# in postgres
# create table summoners(summonerid varchar primary key, summonername varchar, accountid varchar unique, puuid varchar unique, region varchar, tier varchar, rank int, last_updated timestamp);
