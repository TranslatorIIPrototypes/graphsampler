from redis import Redis
import ast,json,itertools
from collections import defaultdict

class redisinterface():
    def __init__(self,host,port):
        self.r = Redis(host=host, port=port, db=0)
        self.type_r = Redis(host=host, port=port, db=1)

    def get_neighborhood_and_directs(self,nodeids,a_label,b_label,badnodes):
        ##
        #  This function only works for degree = 2.
        #  It first gets the degree = 1 nodes (D1) for each endpoint
        #  Then it gets the neighbors for each D1 point
        #  These will be in two categories: if they already a D1 point, then the edge returned is an edge we want
        #    if they are not a D1 point, then we only want it if the new point comes up as a neighbor to a D1 point
        #    for each endpoint (i.e. the new point is a join, and potentially the middle node of a 3 node path).
        ##
        print(nodeids)
        a_id = nodeids[0]
        b_id = nodeids[1]
        nodes2edgesa = self.get_neighbors([a_id],badnodes)
        nodes2edgesb = self.get_neighbors([b_id],badnodes)
        if (len(nodes2edgesa) == 0) or (len(nodes2edgesb) == 0):
            print("no neighbors",a_id,b_id)
            return {a_id:a_label, b_id:b_label},[]
        all_one_nodes = set([a_id,b_id])
        internal_edges = []
        add_nodes_and_edges(all_one_nodes,internal_edges,nodes2edgesa)
        add_nodes_and_edges(all_one_nodes,internal_edges,nodes2edgesb)
        neighbors2_a = self.get_neighbors(nodes2edgesa.keys(),badnodes)
        neighbors2_b = self.get_neighbors(nodes2edgesb.keys(),badnodes)
        all_two_nodes = set()
        what = all_one_nodes.intersection(all_two_nodes)
        if len(what) > 0:
            print('what')
        parse_2_neighbors(neighbors2_a, neighbors2_b, all_one_nodes, all_two_nodes, internal_edges)
        parse_2_neighbors(neighbors2_b, neighbors2_a, all_one_nodes, all_two_nodes, internal_edges)
        print(len(all_one_nodes),len(all_two_nodes),a_id in all_one_nodes,b_id in all_one_nodes)
        nodetypes = self.get_nodetypes(all_one_nodes.union(all_two_nodes))
        return nodetypes,internal_edges

    def get_nodetypes(self,nodes):
        with self.type_r.pipeline() as pipe:
            for n_id in nodes:
                pipe.get(n_id)
            pips = pipe.execute()
        results = { n:picklabel(ast.literal_eval(x.decode())) for n,x in zip(nodes,pips) }
        return results

    def get_neighbors(self,node_identifiers,bad_neighbors):
        results = []
        pipelength = 20
        with self.r.pipeline() as pipe:
            for ni in grouper(pipelength, node_identifiers):
                for hub in ni:
                    pipe.get(hub)
                results += pipe.execute()
        neighbors_and_edges = {}
        for n,x in zip(node_identifiers,results):
            neighbors_and_edges.update(convert(x,n,bad_neighbors) )
        return neighbors_and_edges

def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

def picklabel(labellist):
    for leaf in ['gene','chemical_substance','disease','phenotypic_feature','cell','cellular_component','biological_process','molecular_activity','organism_taxon','sequence_variant','gene_family','environmental_feature','population_of_individual_organisms']:
        if leaf in labellist:
            return leaf
    for leaf in ['anatomical_entity']:
        if leaf in labellist:
            return leaf
    if len(labellist) == 1 and 'named_thing' in labellist:
        return 'named_thing'
    print('crapaddidle')
    print(labellist)
    exit()

def parse_2_neighbors(n2a,n2b,all_one_nodes,all2nodes,internal_edges):
    for n2,newedges in n2a.items():
        #Maybe this isn't a new node, but we do want the edges
        if n2 in all_one_nodes:
            internal_edges += newedges
        #Maybe this is just sticking out into space - screw it
        elif n2 not in n2b:
            continue
        #But if it is in the other 2 hop neighborlist, then it is the fabled linker node.
        else:
            all2nodes.add(n2)
            internal_edges += newedges

def add_nodes_and_edges(outnodes,outedges,nodes2edges):
    for node,edges in nodes2edges.items():
        outnodes.add(node)
        outedges += edges

def convert(x,query_node,crap):
    nodes2edges = defaultdict(list)
    if x is not None:
        redges = json.loads(x)
        for t in redges:
            node = t[0]
            if node in crap:
                continue
            pred = t[1]
            if pred not in ['subclass_of','expresses','in_taxon']:
                forward = t[2]
                if forward:
                    nodes2edges[node].append( (query_node,node,{'predicate':pred}) )
                else:
                    nodes2edges[node].append( (node,query_node,{'predicate':pred}) )
    return nodes2edges
