import networkx

def get_leaf(labellist):
    for leaf in ['gene','chemical_substance','disease','phenotypic_feature','cell','cellular_component','taxon','sequence_variant']:
        if leaf in labellist:
            return leaf
    for leaf in ['anatomical_entity']:
        if leaf in labellist:
            return leaf
    print('crapaddidle')
    print(labellist)
    exit()

class NGraph:
    def __init__(self,nodes,edges,ids,predicate):
        #Expects that edges is in the form of a list of triples (source_id, target_id, {'predicate':predicate})
        self.ngraph = networkx.MultiDiGraph()
        tedges = [ (x,y,z) for x,y,z in edges if ( (x in nodes) and (y in nodes) )]
        self.ngraph.add_edges_from(tedges)
        self.source = ids[0]
        self.target = ids[1]
        self.predicate = predicate
        for nid, nl in nodes.items():
            label = get_leaf(nl)
            self.ngraph.nodes[nid]['label'] = label
    def create_cyphers(self):
        q = 'MATCH \n'
        cedges = []
        for edge in self.ngraph.edges(data=True):
            s = edge[0]
            t = edge[1]
            p = edge[2]['predicate']
            cedges.append( f'({s}:{self.ngraph.nodes[s]["label"]})-[:{p}]->({t}:{self.ngraph.nodes[t]["label"]})' )
        q += ',\n'.join(cedges)
        p = q + f',\n({self.source})-[:{self.predicate}]->({self.target})'
        r = f'\nwith {self.source}, {self.target}, count(*) as ignore return count(*)'
        q += r
        p += r
        return q,p