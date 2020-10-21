import networkx as nx
from counter import compact_graph

def test_simple():
    g = nx.DiGraph()
    g.add_edges_from([(1,2),(1,3)],predicate='x')
    g.add_edges_from([(2,4),(3,4)],predicate='y')
    assert g.number_of_nodes() == 4
    assert g.number_of_edges() == 4
    q = compact_graph(g)
    assert q.number_of_nodes() == 3
    assert q.number_of_edges() == 2
    for node in q.nodes():
        print(next(iter(node)))

def test_fail():
    g = nx.DiGraph()
    #3,1 is reversed.  Now 2 and 3 have different successors / predecessors
    g.add_edges_from([(1, 2), (3, 1)], predicate='x')
    g.add_edges_from([(2, 4), (3, 4)], predicate='y')
    assert g.number_of_nodes() == 4
    assert g.number_of_edges() == 4
    q = compact_graph(g)
    assert q.number_of_nodes() == 4
    assert q.number_of_edges() == 4

def test_fail_predicate():
    g = nx.DiGraph()
    #1,2 and 1,3 have different predicates, so 2,3 are not good
    g.add_edge(1, 2, predicate='x')
    g.add_edge(1, 3, predicate='z')
    g.add_edges_from([(2, 4), (3, 4)], predicate='y')
    assert g.number_of_nodes() == 4
    assert g.number_of_edges() == 4
    q = compact_graph(g)
    assert q.number_of_nodes() == 4
    assert q.number_of_edges() == 4

