import sys
import argparse
import random
import json
from neo_interface import neo
from counter import counter,writer

crummy_nodes = set([
    "CHEBI:5686",   # heterocyclic compound
    "CHEBI:18059",   # lipid
    "CHEBI:23367",  # "molecular entity"
    "CHEBI:24431",  # "chemical entity"
    "CHEBI:24538",  # "organic heterocyclic compound"
    "CHEBI:24651",  # hydroxides
    "CHEBI:25367",  # molecule
    "CHEBI:25703",  # "organic phosphate
    "CHEBI:25806",  # oxygen molecular entity
    "CHEBI:32988",  #amide
    "CHEBI:33256",  # primary amide
    "CHEBI:33285",  # heteroorganic
    "CHEBI:33302",  # pnictogen molecular entity
    "CHEBI:33304",  # chalcogen molecular entity
    "CHEBI:33326",  # nickel group element atom
    "CHEBI:33561",  # d-block element atom
    "CHEBI:33579",  # "main group molecular entity"
    "CHEBI:33582",  # carbon group molecular entity
    "CHEBI:33595", #cyclic compound
    "CHEBI:33597", # homocyclic compound
    "CHEBI:33608", #hydrogen molecular entity
    "CHEBI:33635", #polycyclic compound
    "CHEBI:33636", #bicyclic compound
    "CHEBI:33655", #aromatic compound
    "CHEBI:33659", #organic aromatic compound
    "CHEBI:33674", #s-block molecular entity
    "CHEBI:33675", #p-block molecular entity
    "CHEBI:33832", #organic cyclic compound
    "CHEBI:35294", #carbopolyciclic compound
    "CHEBI:35352", #organonitrogen compound
    "CHEBI:36357", #polyatomic entity
    "CHEBI:36586", #carbonyl compund
    "CHEBI:36587", #organic oxo compound
    "CHEBI:36962", #organochalcogen compound
    "CHEBI:36963", #organooxygen compound
    "CHEBI:37577", #heteroatomic compound
    "CHEBI:37622", #carboxamide
    "CHEBI:38101", #organonitrogen hetrocyclic compound
    "CHEBI:38116", #organice heteropolycyclic compound
    "CHEBI:50860", #organic molecular entity
    "CHEBI:51143", #nitrogen molecular entity
    "CHEBI:72695", #organic molecule
    "CHEBI:78616", #carbohydrates and carbohydrate derivatives
    ] )

def build_stoch_nodes(atype,btype,connected,npairs,outprefix):
    with open('conn.json','r') as inf:
        conn_data = json.load(inf)
    n = neo(conn_data['neouri'],conn_data['neouser'],conn_data['neopass'])
    w = writer(outprefix)
    c = counter(w)
    if not connected:
        a_nodes = n.get_interesting_nodes_by_type(atype)
        filternodes(a_nodes)
        if atype == btype:
            b_nodes = a_nodes
        else:
            b_nodes = n.get_interesting_nodes_by_type(btype)
        filternodes(b_nodes)
        print("node counts:", len(a_nodes), len(b_nodes))
        ak = list(a_nodes.keys())
        bk = list(b_nodes.keys())
        for pcount in range(npairs):
            a = random.choice(ak)
            b = random.choice(bk)
            nodes,edges = n.get_neighborhood_and_directs((a,b),atype,btype,crummy_nodes,degree=2)
            c.count((a,b),nodes,edges)
    else:
        #get all the pairs, even if we don't want to run them all
        allpairs = n.get_pairs(atype,btype)
        random.shuffle(allpairs)
        ndone = 0
        print('npairs:',len(allpairs))
        for ab in allpairs:
            nodes, edges = n.get_neighborhood_and_directs(ab, atype, btype, crummy_nodes, degree=2)
            c.count(ab, nodes, edges)
            ndone += 1
            if ndone >= npairs:
                break

def filternodes(in_nodes):
    for c in crummy_nodes:
        if c in in_nodes:
            in_nodes.pop(c)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', action='store', dest='a_type',
                        help='Node Type A')
    parser.add_argument('-b', action='store', dest='b_type',
                        help='Node Type B')
    parser.add_argument('-c', action='store_true', dest='connected',
                        default=False,
                        help='Pull connected nodes')
    parser.add_argument('-n', action='store', type=int,
                        dest='numpairs',
                        help='Number of pairs to pull')
    parser.add_argument('-o', action='store',
                        dest='output_prefix',
                        help='prefix for output files')

    results = parser.parse_args()
    build_stoch_nodes(results.a_type, results.b_type, results.connected, results.numpairs, results.output_prefix)
