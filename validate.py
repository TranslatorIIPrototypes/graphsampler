from reasoner.cypher import get_query
from neo_interface import neo
import pandas as pd
import json
from collections import defaultdict

def get_neo():
    with open('conn.json','r') as inf:
        conn_data = json.load(inf)
    return neo(conn_data['neouri'],conn_data['neouser'],conn_data['neopass'])

def validate_all(start_type,end_type):
    indir = f'{start_type}_{end_type}'
    n = get_neo()
    df = pd.read_csv(f'{indir}/paretographs',sep='\t')
    all_connected=[]
    all_starts  = n.get_interesting_nodes_by_type(start_type)
    pairs = n.get_pairs(start_type,end_type)
    all_connected = defaultdict(set)
    for x,y in pairs:
        all_connected[x].add(y)
    for index,row in df.iterrows():
        validate_one(index,row['graph'],start_type,end_type,all_starts,all_connected,n)

def validate_one(rowindex,s_trapi,start_type,end_type,all_starts,all_connected,neo_i):
    trapi = json.loads(s_trapi)
    start_id = f'input_{start_type}'
    end_id = f'input_{end_type}'
    for node in trapi['nodes']:
        if node['id'] == end_id:
            del node['curie']
    with open(f'{start_type}_{end_type}/precisionrecall_{rowindex}','w') as outf:
        for start_curie in all_starts:
            for node in trapi['nodes']:
                if node['id'] == start_id:
                    node['curie'] = start_curie
            cypher = get_query(trapi)
            newcypher = cypher.split('WITH')[0] + f' RETURN DISTINCT input_{end_type}.id as output'
            matches = neo_i.run_query(newcypher)
            for match in matches:
                outf.write(f'{start_curie}\t{match}\t{match in all_connected[start_curie]}\n')

if __name__ == '__main__':
    validate_all('gene','disease')
