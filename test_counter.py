from neo_interface import neo
from counter import counter, node_combs

def test_missing():
    neouri = "bolt://localhost:7687"
    neouser = "neo4j"
    neopass = "***REMOVED***"
    n = neo(neouri, neouser, neopass)
    ab=['CHEMBL.COMPOUND:CHEMBL2333610', 'CHEBI:86216']
    nodes, edges = n.get_neighborhood_and_directs(ab, 'chemical_substance', 'chemical_substance', degree=1)
    c = counter(n)
    c.count(ab, nodes, edges, 'chemical_substance', 'chemical_substance')

def test_combination():
    n3 = [[1,2,3],[1,2,4]]
    n2 = []
    n1 = []
    x = 0
    for c in node_combs(n3,n2,n1):
        x  += 1
    assert x == 2

def test_combination_2():
    n3 = [[1, 2, 3]]
    n2 = [[1,3]]
    n1 = [[4]]
    x = 0
    for c in node_combs(n3, n2, n1):
        x += 1
    assert x == 2


def test_combination_3():
    n3 = []
    n2 = [[1, 2],[1, 3]]
    n1 = []
    x = 0
    for c in node_combs(n3, n2, n1):
        x += 1
    assert x == 1

def test_combination_4():
    n3 = []
    n2 = [[1, 2],[1, 3]]
    n1 = [[4]]
    x = 0
    for c in node_combs(n3, n2, n1):
        x += 1
    assert x == 3
