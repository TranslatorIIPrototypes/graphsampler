#from math import comb
from graph import NGraph
from itertools import combinations

class degrader:
    def __init__(self,n):
        self.neo = n
        pass

    def degrade(self,nodes,edges,edgetype,end_ids,querysize=3):
        #the nodes and edges are curies.  I want to use those as the node identifiers in cypher, but I can't
        # with the ":" in them, so let's remove them
        newnodes = { '_'.join(n.split(':')):v for n,v in nodes.items()}
        newedges = [ ('_'.join(x.split(':')),('_'.join(y.split(':'))),z) for x,y,z in edges
                     if not ((x == end_ids[0]) and (y == end_ids[1]) and (z['predicate']==edgetype))]
        originals = [ '_'.join(n.split(':')) for n in end_ids ]
        inters = set(newnodes.keys()).difference(originals)
        numcombs = comb(len(inters),querysize)
        print(originals)
        print('Number of new nodes:', len(inters))
        print('Number of combinations:', numcombs)
        for ns in combinations(inters, querysize):
            dnodes = { n:newnodes[n] for n in list(ns)+originals }
            original_graph = NGraph(dnodes,newedges,originals,edgetype)
            withoutcypher,withcypher = original_graph.create_cyphers()
            print(withoutcypher)
            withcount = self.neo.execute_counter(withcypher)
            print('x')
            withoutcount = self.neo.execute_counter(withoutcypher)
            print(withcount, withoutcount, withcount / (withoutcount+withcount))
