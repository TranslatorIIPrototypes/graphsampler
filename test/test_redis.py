from redis_interface import redisinterface
import json

def test_n2():
    with open('../conn.json','r') as inf:
        conn_data = json.load(inf)
    r = redisinterface(conn_data['redisuri'],conn_data['redisport'])
    r.get_neighborhood_and_directs(['NCBIGene:105374454','MONDO:0001503'],'gene','disease',[])
