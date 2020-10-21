from counter import counter
from collections import defaultdict

class test_writer():
    """A mock writer for counter so that we can collect the graphs that show up"""
    def __init__(self):
        self.graphs = []
    def write_graph(self, graph):
        self.graphs.append(graph)
    def write_counts(self, s):
        pass
    def __del__(self):
        pass

#redo with fixtures
def test_bare_three_hop():
    """We pass in a graph with only 3 nodes and they make a single three hop.  It should write one graph."""
    w = test_writer()
    c = counter(w)
    nn = { "curie:a":"gene", "curie:b":"chemical_substance","curie:c":"disease","curie:start":"gene","curie:end":"phenotypic_feature"}
    ends = ["curie:start","curie:end"]
    edges = [("curie:start","curie:a",{"predicate":"p0"}),
             ("curie:a","curie:b",{"predicate":"p1"}),
             ("curie:c", "curie:b", {"predicate": "p0"}),
             ("curie:c", "curie:end", {"predicate": "p1"})]
    c.count(ends,nn,edges)
    assert len(w.graphs) == 1
    g = w.graphs[0]
    assert len(g.nodes) == 5
    assert len(g.edges) == 4

def test_triples():
    """Suppose you have a set of 1 node connections"""
    w = test_writer()
    c = counter(w)
    nn = { "curie:0":"gene",
           "curie:1":"gene",
           "curie:2":"chemical_substance",
           "curie:3":"chemical_substance",
           "curie:4":"disease",
           "curie:5":"disease",
           "curie:start":"gene",
           "curie:end":"phenotypic_feature"}
    ends = ["curie:start","curie:end"]
    edges = []
    for i in range(6):
        edges.append( ('curie:start',f'curie:{i}',{"predicate":"l"}))
        edges.append(('curie:end', f'curie:{i}', {"predicate": "r"}))
    c.count(ends,nn,edges)
    #We should get 7  with 3 nodes:
    #genegenechem, genegenedise, chemchemdise, chemchemgene, disedisegene,disedisechem, genechemdise
    #And 6 with 2 nodes: #genegene, genechem, genedise,chemcehm,chemdise,disedise,
    # and 3 with 1 node
    gcounts=defaultdict(int)
    for g in w.graphs:
        gcounts[len(g.nodes)-2] += 1
        assert len(g.edges) == 2*(len(g.nodes)-2)
    assert gcounts[1] == 3
    assert gcounts[2] == 6
    assert gcounts[3] == 7


def test_repeat():
    """Given start-a-b-end, start-c-end, start-d-end, where c,d have the same type and adjacent predicates,
    return a single graph. i.e. you should get one graph with ab,c.  ab,d is a dupe.
    For 2 node, you should get an a-b, and a c,d, and you should get a single 1 node."""
    w = test_writer()
    c = counter(w)
    nn = {"curie:a": "gene", "curie:b": "chemical_substance",
          "curie:c": "disease",
          "curie:d": "disease",
          "curie:start": "gene",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:a", "curie:b", {"predicate": "p1"}),
             ("curie:end", "curie:b", {"predicate": "p0"}),
             ("curie:c", "curie:start", {"predicate": "p1"}),
             ("curie:c", "curie:end", {"predicate": "p1"}),
             ("curie:d", "curie:start", {"predicate": "p1"}),
             ("curie:d", "curie:end", {"predicate": "p1"})
             ]
    c.count(ends, nn, edges)
    assert len(w.graphs) == 4
    gcounts = defaultdict(int)
    for g in w.graphs:
        gcounts[ (len(g.nodes)-2, len(g.edges))] += 1
    assert gcounts[ (3,5) ] == 1
    assert gcounts[ (2,3) ] == 1
    assert gcounts[ (2,4) ] == 1
    assert gcounts[ (1,2) ] == 1

def test_not_quite_repeat_nodetype():
    """Given start-a-b-end, start-c-end, start-d-end, where c,d have different types
    return 2 3-node graphs, 2 2-nodes, and 2 1-nodes"""
    w = test_writer()
    c = counter(w)
    nn = {"curie:a": "gene", "curie:b": "chemical_substance",
          "curie:c": "disease",
          "curie:d": "gene",
          "curie:start": "gene",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:a", "curie:b", {"predicate": "p1"}),
             ("curie:end", "curie:b", {"predicate": "p0"}),
             ("curie:c", "curie:start", {"predicate": "p1"}),
             ("curie:c", "curie:end", {"predicate": "p1"}),
             ("curie:d", "curie:start", {"predicate": "p1"}),
             ("curie:d", "curie:end", {"predicate": "p1"})
             ]
    c.count(ends, nn, edges)
    assert len(w.graphs) == 6
    gcounts = defaultdict(int)
    for g in w.graphs:
        gcounts[ (len(g.nodes)-2, len(g.edges))] += 1
    assert gcounts[ (3,5) ] == 2
    assert gcounts[ (2,3) ] == 1
    assert gcounts[ (2,4) ] == 1
    assert gcounts[ (1,2) ] == 2

def test_not_quite_repeat_pred():
    """Given start-a-b-end, start-c-end, start-d-end, where c,d have types but a different predicate
    return 2 3-graphs, 2 2-graphs, 2 1-graphs"""
    w = test_writer()
    c = counter(w)
    nn = {"curie:a": "gene", "curie:b": "chemical_substance",
          "curie:c": "disease",
          "curie:d": "disease",
          "curie:start": "gene",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:a", "curie:b", {"predicate": "p1"}),
             ("curie:end", "curie:b", {"predicate": "p0"}),
             ("curie:c", "curie:start", {"predicate": "p1"}),
             ("curie:c", "curie:end", {"predicate": "p1"}),
             ("curie:d", "curie:start", {"predicate": "p2"}),
             ("curie:d", "curie:end", {"predicate": "p1"})
             ]
    c.count(ends, nn, edges)
    assert len(w.graphs) == 6
    gcounts = defaultdict(int)
    for g in w.graphs:
        gcounts[ (len(g.nodes)-2, len(g.edges))] += 1
    assert gcounts[ (3,5) ] == 2
    assert gcounts[ (2,3) ] == 1
    assert gcounts[ (2,4) ] == 1
    assert gcounts[ (1,2) ] == 2

def test_similarity_extra():
    """start-a-end, start-b-end, a-c-b.  This can only happen if you use 2-neighborhoods.
    You should get 1 3node, 1 2node (leaving out c), and 2 one nodes if typea <> typec"""
    w = test_writer()
    c = counter(w)
    nn = {"curie:a": "gene",
          "curie:b": "chemical_substance",
          "curie:c": "disease",
          "curie:start": "gene",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:a", "curie:end", {"predicate": "p1"}),
             ("curie:end", "curie:b", {"predicate": "p0"}),
             ("curie:start", "curie:b", {"predicate": "p0"}),
             ("curie:a", "curie:c", {"predicate": "p1"}),
             ("curie:b", "curie:c", {"predicate": "p0"})
             ]
    c.count(ends, nn, edges)
    gcounts = defaultdict(int)
    assert len(w.graphs) == 4
    for g in w.graphs:
        gcounts[ (len(g.nodes)-2, len(g.edges))] += 1
    assert gcounts[ (3,6) ] == 1
    assert gcounts[ (2,4) ] == 1
    assert gcounts[ (1,2) ] == 2

def test_overlapping():
    """If there are 2 2-paths a-b and a-c, then a,b,c are all in the 3-node q graph, and there are 5 edges
    There are also 2 2-graphs (ab and ac). no 1-graphs"""
    w = test_writer()
    c = counter(w)
    nn = {"curie:a": "gene",
          "curie:b": "chemical_substance",
          "curie:c": "disease",
          "curie:start": "gene",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:a", "curie:b", {"predicate": "p1"}),
             ("curie:end", "curie:b", {"predicate": "p0"}),
             ("curie:a", "curie:c", {"predicate": "p1"}),
             ("curie:end", "curie:c", {"predicate": "p0"})
             ]
    c.count(ends, nn, edges)
    assert len(w.graphs) == 3
    gc = defaultdict(int)
    for g in w.graphs:
        nn = len(g.nodes)-2
        ne = len(g.edges)
        gc[ (nn,ne) ] += 1
    assert gc[(3,5)] == 1
    assert gc[(2,3)] == 2

def test_4node_fully_connected():
    """If we have start, end, a,b,c,d and everything is connected (and a,b,c,d all diff types), we should get 4 3-graphs.
    Each should leave 1 out
    Also 4 choose 2 = 6 2-graphs and 4 1-graphs
    """
    w = test_writer()
    c = counter(w)
    nn = {"curie:start": "gene",
          "curie:b": "chemical_substance",
          "curie:c": "disease",
          "curie:d": "phenotypic_feature",
          "curie:a": "gene",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    allnodes = list(nn.keys())
    edges=[]
    for i,n0 in enumerate(allnodes):
        for n1 in allnodes[i+1:]:
            if not ((n0 == 'curie:start') and (n1 == 'curie:end')):
                edges.append( (n0,n1,{"predicate":"p0"}))
    c.count(ends, nn, edges)
    assert len(w.graphs) == 14
    gc = defaultdict(int)
    for g in w.graphs:
        nn = len(g.nodes)-2
        ne = len(g.edges)
        gc[ (nn,ne) ] += 1
    assert gc[(3,9)] == 4
    assert gc[(2,5)] == 6
    assert gc[(1,2)] == 4


def test_4node_2_subs():
    """start-a-b-end, a-c-b, a-d-end , should be 2, one with abc, one with abc, one with abd"""
    w = test_writer()
    c = counter(w)
    nn = {"curie:start": "gene",
          "curie:a": "gene",
          "curie:b": "chemical_substance",
          "curie:c": "disease",
          "curie:d": "phenotypic_feature",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    allnodes = list(nn.keys())
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:a", "curie:b", {"predicate": "p1"}),
             ("curie:end", "curie:b", {"predicate": "p0"}),
             ("curie:a", "curie:c", {"predicate": "p1"}),
             ("curie:b", "curie:c", {"predicate": "p0"}),
             ("curie:a", "curie:d", {"predicate": "p1"}),
             ("curie:d", "curie:end", {"predicate": "p2"}),
             ]
    c.count(ends, nn, edges)
    assert len(w.graphs) == 4
    gc = defaultdict(int)
    for g in w.graphs:
        nn = len(g.nodes)-2
        ne = len(g.edges)
        gc[ (nn,ne) ] += 1
    assert gc[(3,5)] == 2
    assert gc[(2,3)] == 2

def test_4node_3_subs():
    """same as 4node_2_subs, but add a c-d edge which should add one more graph"""
    w = test_writer()
    c = counter(w)
    nn = {"curie:start": "gene",
          "curie:a": "gene",
          "curie:b": "chemical_substance",
          "curie:c": "disease",
          "curie:d": "phenotypic_feature",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    allnodes = list(nn.keys())
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:a", "curie:b", {"predicate": "p1"}),
             ("curie:end", "curie:b", {"predicate": "p0"}),
             ("curie:a", "curie:c", {"predicate": "p1"}),
             ("curie:b", "curie:c", {"predicate": "p0"}),
             ("curie:a", "curie:d", {"predicate": "p1"}),
             ("curie:d", "curie:end", {"predicate": "p2"}),
             ("curie:c", "curie:d", {"predicate": "p0"})
             ]
    c.count(ends, nn, edges)
    assert len(w.graphs) == 5
    gc = defaultdict(int)
    for g in w.graphs:
        nn = len(g.nodes)-2
        ne = len(g.edges)
        gc[ (nn,ne) ] += 1
    assert gc[(3,5)] == 3
    assert gc[(2,3)] == 2

def test_simple_1hop():
    """Can we find graphs that are just a single one-hop?"""
    w = test_writer()
    c = counter(w)
    nn = {"curie:start": "gene",
          "curie:a": "gene",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    allnodes = list(nn.keys())
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:a", "curie:end", {"predicate": "p2"})
             ]
    c.count(ends, nn, edges)
    assert len(w.graphs) == 1
    for g in w.graphs:
        assert len(g.nodes) == 3

def test_simple_2hop():
    """Can we find graphs that are just a single two-hop?"""
    w = test_writer()
    c = counter(w)
    nn = {"curie:start": "gene",
          "curie:a": "gene",
          "curie:b": "disease",
          "curie:end": "phenotypic_feature"}
    ends = ["curie:start", "curie:end"]
    allnodes = list(nn.keys())
    edges = [("curie:start", "curie:a", {"predicate": "p0"}),
             ("curie:b", "curie:a", {"predicate": "p0"}),
             ("curie:b", "curie:end", {"predicate": "p2"})
             ]
    c.count(ends, nn, edges)
    assert len(w.graphs) == 1
    for g in w.graphs:
        assert len(g.nodes) == 4
