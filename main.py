import sys
import random
import json
from neo_interface import neo
from degrader import degrader
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

def run(source_label,edge_type,target_label,neouri,neouser,neopass):
    n = neo(neouri,neouser,neopass)
    pairs = n.get_pairs(source_label,edge_type,target_label,maxpairs=100)
    print(len(pairs))
    d = degrader(n)
    for pair in pairs:
        nodes,edges = n.get_neighborhood(source_label,target_label,pair,degree=1)
        print(len(nodes))
        print(len(edges))
        d.degrade(nodes,edges,edge_type,pair)

def build_stoch_nodes(neouri,neouser,neopass,atype,btype,connected,npairs=100000):
    n = neo(neouri,neouser,neopass)
    allnodes = n.get_interesting_nodes(atype,btype,connected)
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
    build_stoch('gene','disease',True)

