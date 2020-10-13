import sys
import random
import json
from neo_interface import neo
#from degrader import degrader
from counter import counter

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

def build_stoch_nodes(atype,btype,connected,npairs=100000):
    with open('conn.json','r') as inf:
        conn_data = json.load(inf)
    n = neo(conn_data['neouri'],conn_data['neouser'],conn_data['neopass'])
    c = counter(n)
    if not connected:
        a_nodes = n.get_interesting_nodes_by_type(atype)
        filternodes(a_nodes)
        if atype == btype:
            b_nodes = a_nodes
        else:
            b_nodes = n.get_interesting_nodes_by_type(btype)
        filternodes(b_nodes)
        ak = list(a_nodes.keys())
        bk = list(b_nodes.keys())
        for pcount in range(npairs):
            a = random.choice(ak)
            b = random.choice(bk)
            nodes,edges = n.get_neighborhood_and_directs((a,b),atype,btype,crummy_nodes,degree=1)
            c.count((a,b),nodes,edges,atype,btype)
    else:
        #get all the pairs, even if we don't want to run them all
        allpairs = n.get_pairs(atype,btype)
        random.shuffle(allpairs)
        ndone = 0
        print('npairs:',len(allpairs))
        for ab in allpairs:
            nodes, edges = n.get_neighborhood_and_directs(ab, atype, btype, crummy_nodes, degree=1)
            c.count(ab, nodes, edges, atype, btype)
            ndone += 1
            if ndone >= npairs:
                break


def build_stoch(npairs=1000000):
    with open('conn.json','r') as inf:
        conn_data = json.load(inf)
    n = neo(conn_data['neouri'],conn_data['neouser'],conn_data['neopass'])
    allnodes = n.get_interesting_nodes()
    print(len(allnodes))
    filternodes(allnodes)
    print(len(allnodes))
    c = counter(n)
    for pcount in range(npairs):
        ab = random.sample(allnodes.keys(),k=2)
        a_label=allnodes[ab[0]]
        b_label=allnodes[ab[1]]
        #ab = ['CHEBI:45906','MONDO:0002367']
        #a_label = 'chemical_substance'
        #b_label = 'disease'
        print(ab)
        nodes,edges = n.get_neighborhood_and_directs(ab,a_label,b_label,crummy_nodes,degree=1)
        c.count(ab,nodes,edges,a_label,b_label)

def filternodes(in_nodes):
    for c in crummy_nodes:
        if c in in_nodes:
            in_nodes.pop(c)

if __name__ == '__main__':
    build_stoch_nodes('gene','disease',True,npairs=1)

