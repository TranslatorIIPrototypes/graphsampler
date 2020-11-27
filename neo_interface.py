from neo4j import GraphDatabase

class neo():
    def __init__(self,uri,user,pw):
        if user == '':
            self.driver = GraphDatabase.driver(uri)
        else:
            self.driver = GraphDatabase.driver(uri, auth=(user,pw))

    def run_query(self,q):
        with self.driver.session() as session:
            res = session.read_transaction(rq,q)
        return res

    def get_pairs(self,sourcelabel,targetlabel,maxpairs=0):
        with self.driver.session() as session:
            friends = session.read_transaction(query_pairs, sourcelabel,targetlabel,maxpairs)
        return friends

    def get_neighborhood(self,a_label,b_label,ids,degree=1):
        a_id,b_id = ids
        nodes = {}
        with self.driver.session() as session:
            #First collect all the nodes that are involved in paths between a and b (up to length degree).
            for d in range(1,degree+1):
                newnodes = session.read_transaction(get_hops,a_label,a_id,b_label,b_id,d)
                for nid,nlabel in newnodes:
                    nodes[nid] = nlabel
            #Now get all the edges connecting all these nodes
            edges = session.read_transaction(get_edges,nodes.keys(),ids[0],ids[1])
        return nodes,edges

    def get_neighborhood_and_directs(self,nodeids,a_label,b_label,badnodes,degree=1):
        print(nodeids)
        if degree > 2:
            print("We're not ready for that!")
            exit()
        nodes = {}
        a_id = nodeids[0]
        b_id = nodeids[1]
        with self.driver.session() as session:
            #First collect all the nodes that are involved in paths between a and b (up to length degree).
            for xid,xlabel in [(a_id,a_label),(b_id,b_label)]:
                #We're limiting to degree here.
                newnodes = session.read_transaction(get_node_neighborhood,xid,xlabel,1)
                for nid,nlabel in newnodes:
                    if nid not in badnodes:
                        nodes[nid] = nlabel
                print('nodes',len(nodes))
            if degree == 2:
                newnodes = session.read_transaction(get_joiners,a_id,a_label,b_id,b_label)
                for nid,nlabel in newnodes:
                    if nid not in badnodes:
                        nodes[nid] = nlabel
            print('Nodes',len(nodes))
            #Now get all the edges connecting all these nodes
            edges = session.read_transaction(get_edges,nodes.keys(),nodeids)
            print('Edges',len(edges))
        return nodes,edges

    def execute_counter(self,cypher):
        with self.driver.session() as session:
            record = session.read_transaction(ex,cypher)
        return record['count(*)']

    def get_interesting_nodes(self):
        with self.driver.session() as session:
            nodes = session.read_transaction(get_interesting_nodes)
        return nodes

    def get_interesting_nodes_by_type(self,type):
        with self.driver.session() as session:
            nodes = session.read_transaction(get_interesting_nodes_by_type,type)
        return nodes

def rq(tx,cypher):
    result = tx.run(cypher)
    results = [ r['output'] for r in result ]
    return results

def ex(tx,cypher):
    result = tx.run(cypher)
    for record in result:
        return record

def query_pairs(tx, sourcelabel, targetlabel, maxpairs=0):
    pairs = []
    q = f"MATCH (a:{sourcelabel})--(b:{targetlabel})" \
         "where not a:Concept and not a.id starts with 'UniProt' " \
         "and not b:Concept and not b.id starts with 'UniProt' " \
         "RETURN a.id, b.id"
    if maxpairs > 0:
        q += f' LIMIT {maxpairs}'
    result = tx.run(q)
    for record in result:
        pairs.append((record['a.id'],record['b.id']))
    return pairs

def get_interesting_nodes(tx):
    ###  Depends on what we want.  ATM, we have a bunch of viral proteomes that don't link to much else, so not interested
    ### also a bunch of variants that I might be interested in, but not in establishing relations to other entities.j
    q = 'MATCH (n) where not n:Concept and not n:sequence_variant and not n:gene_family and not n:organism_taxon ' \
        'and not n:environmental_feature and not n:population_of_individual_organisms ' \
        'and not n:biolink ' \
        'and not n.id starts with "UniProt" ' \
        'and size((n)-[]-()) > 0 ' \
        'RETURN n.id, labels(n)'
        #'RETURN n.id, labels(n) LIMIT 100'
    result = tx.run(q)
    nodes = { r['n.id']:picklabel(r['labels(n)']) for r in result }
    nodes = { a:b for a,b in nodes.items() if b != 'named_thing'}
    return nodes

def get_interesting_nodes_by_type(tx,type):
    ###  Depends on what we want.  ATM, we have a bunch of viral proteomes that don't link to much else, so not interested
    ### also a bunch of variants that I might be interested in, but not in establishing relations to other entities.j
    q = f'MATCH (n:{type}) where not n:Concept ' \
        'and not n.id starts with "UniProt" ' \
        'and size((n)-[]-()) > 0 ' \
        'RETURN n.id, labels(n)'
    result = tx.run(q)
    nodes = { r['n.id']:picklabel(r['labels(n)']) for r in result }
    nodes = { a:b for a,b in nodes.items() if b != 'named_thing'}
    return nodes


def get_hops(tx, source_label, source_id, target_label, target_id, inodecount):
    q = f'match p=(a:{source_label} {{id:"{source_id}"}})--'
    for i in range(inodecount):
        q += f'(x_{i})--'
    q+=f'(b:{target_label} {{id:"{target_id}"}}) with nodes(p) as np unwind np as n return distinct n.id, labels(n)'
    result = tx.run(q)
    node_ids = [ (r['n.id'],r['labels(n)']) for r in result ]
    return node_ids

def get_node_neighborhood(tx, source_id, source_label, nh):
    q = f'match p = (a:{source_label} {{id:"{source_id}"}})-[*0..{nh}]-(n) where none(rel in relationships(p) WHERE type(rel) = "subclass_of") with nodes(p) as ns unwind ns as n RETURN distinct n.id, labels(n)'
    result = tx.run(q)
    node_ids = [ (r['n.id'],picklabel(r['labels(n)'])) for r in result ]
    return node_ids

def get_joiners(tx, source_id, source_label, target_id, target_label):
    q = f'match (a:{source_label} {{id:"{source_id}"}})-[w]-(n0)-[x]-(n1)-[y]-(n2)-[z]-(b:{target_label} {{id:"{target_id}"}}) where '  \
        + f' type(w) <> "subclass_of" and type(x) <> "subclass_of" and type(y) <> "subclass_of" and type(z) <> "subclass_of" ' \
        + f' and not (a)--(n1) and not (b)--(n1) return distinct n1.id, labels(n1)'
    result = tx.run(q)
    node_ids = [ (r['n1.id'],picklabel(r['labels(n1)'])) for r in result ]
    return node_ids

def get_edges(tx, nodeset, originals ):
    nodes = ','.join([ f'"{x}"' for x in nodeset] + [f'"{x}"' for x in originals])
    q = f'MATCH (node:named_thing) USING INDEX node:named_thing(id) ' \
        f'WHERE node.id in [{nodes}] WITH collect(node) as nodes CALL apoc.algo.cover(nodes) yield rel ' \
        f'RETURN startNode(rel).id as source, endNode(rel).id as target, type(rel) as edge'
    result = tx.run(q)
    #This format makes it easy to instantiate a networkx graph
    #We're removing subclass of edges here, because it makes the network large for little obvious benefit.  It's too
    # easy to have two things that are subclass of molecule etc.   But it might be too restrictive, as this will never
    # give us a graph with a subclass edge, in which case, what are they for?
    # Note that whatever we take out of edges here also needs to come out of node neighborhood
    results = [ (r['source'],r['target'],{'predicate':r['edge']}) for r in result if not r['edge'] == 'subclass_of']
    return results

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
