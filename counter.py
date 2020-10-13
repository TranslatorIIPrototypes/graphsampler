#from math import comb
from itertools import combinations
import networkx
import json
from collections import defaultdict

class counter:
    def __init__(self,n):
        self.neo = n
        self.written = set()
        self.graphs_output = open('graphs','w')
        self.counts_output = open('counts','w')
        pass

    def __del__(self):
        self.graphs_output.close()

    def count(self,end_ids,nodes,edges,a_label,b_label,querysize=3):
        #the nodes and edges are curies.  I want to use those as the node identifiers in cypher, but I can't
        # with the ":" in them, so let's remove them
        newnodes = { '_'.join(n.split(':')):v for n,v in nodes.items()}
        newedges = [ ('_'.join(x.split(':')),('_'.join(y.split(':'))),z) for x,y,z in edges ]
        #Now, you can have more than one edge between two nodes, but we can't do hash on a multigraph.
        # So let's combine edges
        edgedict = defaultdict(set)
        for x,y,z in newedges:
            edgedict[(x,y)].add(z['predicate'])
        newedges = []
        for (x,y),z in edgedict.items():
            z = list(z)
            z.sort()
            newedges.append( (x,y,{'predicate':'|'.join(z)}))
        originals = [ '_'.join(n.split(':')) for n in end_ids ]
        sources = set( [ x[0] for x in newedges ] + [x[1] for x in newedges] )
        print(f'Originally have {len(newnodes)}')
        fnodes = filter_nodes(newnodes,newedges,originals,querysize)
        print(f'After de-hairing have {len(fnodes)}')
        dedges = get_direct_edges(newedges,originals)
        if len(fnodes) == 0:
            graph = networkx.DiGraph()
            graph.add_node(originals[0],label=f'{a_label}_input')
            graph.add_node(originals[1],label=f'{b_label}_input')
            self.write(graph,dedges,originals[0],a_label,originals[1],b_label)
            return
        #numcombs = comb(len(fnodes),querysize)
        print('Number of new nodes:', len(fnodes))
        #print('Number of possible combinations:', numcombs)
        wrote = False
        n= 0
        bum = 0
        #This is wrong. Instead, we need to make a graph, find all simple paths of length 1, ... q
        # For q = 3, we look for paths length 1,2,3
        # we can make sets of 3 by looking at
        # 1 3-path
        # 1 2-path, 1 1-path
        # 2 2-paths (because of overlaps)
        # 3 1-paths
        paths = make_paths(fnodes+originals,originals,newedges,q=querysize)
        pathbylength=defaultdict(set)
        for path in paths:
            path.remove(originals[0])
            path.remove(originals[1])
            pathbylength[len(path)].add(tuple(path))
        n3 = len(pathbylength[3])
        n2 = len(pathbylength[2])
        n1 = len(pathbylength[1])
        #realcombs = n3 + comb(n2, 2) + n2*n1 + comb(n1,3)
        #print('Number of actual combinations:', realcombs)
        #print(n3,comb(n2,2),n2*n1,comb(n1,3))
        indy_nodes = group_paths(pathbylength[3],pathbylength[2],pathbylength[1],newnodes,newedges,originals[0],originals[1])
        print('numbcomb:',len(indy_nodes))
        #now these are all the actual, independent sets of 3 nodes that we need to make graphs out of.
        #numcombs = comb(len(fnodes),querysize)
        edgemap = { (x,y):(x,y,z) for x,y,z in newedges }
        edgemap2 = { (y,x):(x,y,z) for x,y,z in newedges }
        edgemap.update(edgemap2)
        for a,b,c in indy_nodes:
            al = [originals[0],a,b,c,originals[1]]
            dnodes = {n: newnodes[n] for n in al}
            nedges = []
            for i in range(5):
                for j in range(i,5):
                    xy = (al[i],al[j])
                    if xy in edgemap:
                        nedges.append(edgemap[xy])
            graph = makegraph(dnodes,nedges,originals,a_label,b_label)
            if graph is not None:
                wrote = True
                self.write(graph,dedges,originals[0],a_label,originals[1],b_label)
            else:
                bum += 1
            n += 1
        if not wrote:
            print('no good ones')
            #write empty?

    def write(self,graph,directs,nodea,labela,nodeb,labelb):
        #for edge in graph.edges(data=True):
        #    print(edge)
        hash = networkx.weisfeiler_lehman_graph_hash(graph, edge_attr='predicate', node_attr='label', iterations=3, digest_size=16)
        graph.graph['hash'] = hash
        if hash not in self.written:
            self.graphs_output.write(json.dumps(networkx.json_graph.node_link_data(graph)))
            self.graphs_output.write('\n')
            self.written.add(hash)
        ab = ','.join([ z['predicate'] for x,y,z in directs if ((x==nodea) and (y==nodeb))])
        ba = ','.join([ z['predicate'] for x,y,z in directs if ((x==nodeb) and (y==nodea))])
        self.counts_output.write(f'{hash}\t{nodea}\t{labela}\t{nodeb}\t{labelb}\t{ab}\t{ba}\n')

def group_paths(n3,n2,n1,nodes,edges,snode,tnode):
    """Just iterating over all possible 3 nodes is too much.  So now we have these paths, but even just iterating
    over all acheivable 3-nodes is too much.  So we want to find 3-nodes that are isomorphic.   So we want to find just
    a set that are non-isomorphic"""
    #Group all the 3's
    #There is a path that goes s-a-b-c-t
    #other possible links: s-b, s-c, a-c, a-t, b-t. So there are 32 possible combos
    # We encode this with a bit-tuple.
    used = set()
    with_three_hop=defaultdict(set)
    for a,b,c in n3:
        sb = sc = ac = at = bt = False
        if (b,c) in n2:
           sb = True
        if (c,) in n1:
            sc = True
        if (a,c) in n2:
            ac = True
        if (a,) in n1:
            at = True
        if (a,b) in n2:
            bt = True
        topology = (sb,sc,ac,at,bt)
        if not sb:
            if not sc:
                if not ac:
                    if not at:
                        if not bt:
                            print(a,b,c)
                            exit()
        with_three_hop[topology].add((a,b,c))
        used.add( frozenset( (a,b,c) ) )
    with_two_hop_tbranch=defaultdict(set)
    with_two_hop_sbranch=defaultdict(set)
    for n2p in combinations(n2,2):
        a,b = n2p[0]
        c,d = n2p[1]
        s = frozenset([a,b,c,d])
        if len(s) != 3:
            continue
        if s in used:
            continue
        #This are sets of 2, there has to be one that's shared.
        if a==c:
            #       b
            # s-(a)    t
            #       d
            if (a,) in n1:  #a-t
                at = True
            else:
                at = False
            with_two_hop_sbranch[at].add((a,(b,d)))
        elif b==d:
            if (b,) in n1:
                bt=True
            else:
                bt = False
            with_two_hop_tbranch[bt].add(((a,c),b))
        else:
            print('wtf')
            print(a,b,c,d)
            exit()
        used.add(s)
    #print('Total 3hops:',len(n3))
    #for t,g in with_three_hop.items():
    #    print(t, len(g))
    #print('2hops')
    #print('sb, with cross',len(with_two_hop_sbranch[True]))
    #print('sb, no cross',len(with_two_hop_sbranch[False]))
    #print('tb, with cross',len(with_two_hop_tbranch[True]))
    #print('tb, no cross',len(with_two_hop_tbranch[False]))
    one_twos = defaultdict(set)
    for a,b in n2:
        for (c,) in n1:
            s = frozenset([a,b,c])
            if not len(s) == 3:
                continue
            if s in used:
                continue
            #The only things left should be s-a-b-t-c-s.  There may also be a-t and s-b links.
            sb = (b,) in n1
            at = (a,) in n1
            topology = (sb,at)
            one_twos[topology].add(((a,b),c))
            used.add(s)
    #for t,r in one_twos.items():
    #    print('1,2',t,len(r))
    onehops = set()
    for a,b,c in combinations(n1,3):
        s = frozenset((a[0],b[0],c[0]))
        if s not in used:
            onehops.add(s)
            used.add(s)
    #print('1hops: ',len(onehops))
    edgemap = {}
    for x,y,z in edges:
        #value is (predicate, direction (True = x->y) )
        edgemap[ (x,y) ] = (z['predicate'],True)
        edgemap[ (y,x) ] = (z['predicate'],False)
    indynodes_3hop = group_by_types_3hop(with_three_hop,nodes,edgemap,snode,tnode)
    indynodes_2hop_s = group_by_types_2hop_s(with_two_hop_sbranch,nodes,edgemap,snode,tnode)
    indynodes_2hop_t = group_by_types_2hop_t(with_two_hop_tbranch,nodes,edgemap,snode,tnode)
    indynodes_1_2hop = group_by_types_1_2hop(one_twos,nodes,edgemap,snode,tnode)
    indynodes_1hops = group_by_types_1hops(onehops,nodes,edgemap,snode,tnode)
    return indynodes_3hop + indynodes_2hop_s + indynodes_2hop_t + indynodes_1_2hop + indynodes_1hops

def group_by_types_1hops(triphops,nodes,edges,snode,tnode):
    #This is just a set of (a,b,c) triples, where each is a one-node hop s-(a,b,c)-t.  There's no connections between
    # a,b,c, so they're 3 independent paths
    groups = defaultdict(set)
    for a,b,c in triphops:
        pa = (nodes[a], edges[ (snode,a)], edges[ (a,tnode)])
        pb = (nodes[b], edges[ (snode,b)], edges[ (b,tnode)])
        pc = (nodes[c], edges[ (snode,c)], edges[ (c,tnode)])
        paths = [pa,pb,pc]
        paths.sort()
        pathtuple = tuple(paths)
        groups[pathtuple].add((a,b,c))
    indy_nodes = []
    for _,nset in indy_nodes:
        indy_nodes.append( next(iter(nset)))
    return indy_nodes

def group_by_types_1_2hop(topodict,nodes,edges,snode,tnode):
    #the inputs here are a 2 path s-a-b-t and a 1 path s-c-t with no a-c or b-c connections
    # there can be s-b or a-t (and these are the two bits of the topology key)
    #e_indexes = [(0,2),(0,3),(1,3),(1,4),(2,4)]
    grouped = defaultdict( lambda: defaultdict ( lambda: defaultdict(set)))
    for topology,matches in topodict.items():
        grouped_by_nodetypes = defaultdict(set)
        for ((a,b),c) in matches:
            types = tuple( ((nodes[a],nodes[b]), nodes[c] ) )
            grouped_by_nodetypes[types].add(((a,b),c))
        for nts,ms in grouped_by_nodetypes.items():
            for ((a,b),c) in ms:
                ns = [snode,a,b,tnode]
                etypes = [ edges[(ns[i],ns[i+1])] for i in range(3) ]
                ns = [snode,c,tnode]
                etypes += [ edges[(ns[i],ns[i+1])] for i in range(2) ]
                if topology[0]:
                    etypes.append( edges[(snode,b)] )
                if topology[1]:
                    etypes.append( edges[(a,tnode)] )
                etuple = tuple(etypes)
                grouped[topology][nts][etuple].add(((a,b),c))
    indy_nodes = []
    for topology,g2 in grouped.items():
        for nts,g3 in g2.items():
            for etuple,g4 in g3.items():
                ((a,b),c) = next(iter(g4))
                indy_nodes.append( (a,b,c) )
    return indy_nodes

def group_by_types_2hop_t(topodict,nodes,edges,snode,tnode):
    # These are constructed as s-(a,b)-c-t, where a,b are a branch
    # if topodict = true, there's also an s-c node.
    # Note that a,b can be symmetric, there's no ordering there.
    # But when we get to edge types, it's important that the s-a edge and a-c edge go together.
    grouped = defaultdict( lambda: defaultdict ( lambda: defaultdict(set)))
    for hasbridge,matches in topodict.items():
        grouped_by_nodetypes = defaultdict(set)
        for (a,b),c in matches:
            tc = nodes[c]
            ts = [(nodes[a],a),(nodes[b],b)]
            ts.sort()
            types = (tuple([ts[0][0],ts[1][0]]),tc)
            ab = tuple([ts[0][1],ts[1][1]])
            grouped_by_nodetypes[types].add((ab,c))
        for nts,ms in grouped_by_nodetypes.items():
            for (a,b),c in ms:
                e0 = edges[ (c,tnode)]
                p1 = ( edges[ (snode,a)], edges[ (a,c)])
                p2 = ( edges[ (snode,b)], edges[ (b,c)])
                ps = [p1,p2]
                #If type(b) != type(a), then p1,p2 are already ordered fine. But if type(b)==type(a) we need to sort
                # them, so that we make sure not to have hidden dupes
                if nts[1][0] == nts[1][1]:
                    ps.sort()
                etypes = [tuple(ps),e0]
                if hasbridge:
                    etypes.append(edges[ (snode,c) ])
                etuple = tuple(etypes)
                grouped[hasbridge][nts][etuple].add( ((a,b),c) )
    indy_nodes = []
    for topology, g2 in grouped.items():
        for nts, g3 in g2.items():
            for etuple, g4 in g3.items():
                ((a, b), c) = next(iter(g4))
                indy_nodes.append((a, b, c))
    return indy_nodes

def group_by_types_2hop_s(topodict,nodes,edges,snode,tnode):
    # These are constructed as s-a-(b,c)-t, where b,c are a branch
    # if topodict = true, there's also an a-t node.
    # Note that b,c can be symmetric, there's no ordering there.
    # But when we get to edge types, it's important that the a-b edge and b-t edge go together.
    grouped = defaultdict( lambda: defaultdict ( lambda: defaultdict(set)))
    for hasbridge,matches in topodict.items():
        grouped_by_nodetypes = defaultdict(set)
        for a,(b,c) in matches:
            ta = nodes[a]
            ts = [(nodes[b],b),(nodes[c],c)]
            ts.sort()
            types = (ta,tuple([ts[0][0],ts[1][0]]))
            bc = tuple([ts[0][1],ts[1][1]])
            grouped_by_nodetypes[types].add((a,bc))
        for nts,ms in grouped_by_nodetypes.items():
            for a,(b,c) in ms:
                e0 = edges[ (snode,a)]
                p1 = ( edges[ (a,b)], edges[ (b,tnode)])
                p2 = ( edges[ (a,c)], edges[ (c,tnode)])
                ps = [p1,p2]
                #If type(b) != type(c), then p1,p2 are already ordered fine. But if type(b)==type(c) we need to sort
                # them, so that we make sure not to have hidden dupes
                if nts[1][0] == nts[1][1]:
                    ps.sort()
                etypes = [e0, tuple(ps)]
                if hasbridge:
                    etypes.append(edges[ (a,tnode) ])
                etuple = tuple(etypes)
                grouped[hasbridge][nts][etuple].add( (a,(b,c)) )
    indy_nodes = []
    for topology, g2 in grouped.items():
        for nts, g3 in g2.items():
            for etuple, g4 in g3.items():
                (a, (b, c)) = next(iter(g4))
                indy_nodes.append((a, b, c))
    return indy_nodes

def group_by_types_3hop(topodict,nodes,edges,snode,tnode):
    #s - b, s - c, a - c, a - t, b - t
    e_indexes = [(0,2),(0,3),(1,3),(1,4),(2,4)]
    grouped = defaultdict( lambda: defaultdict ( lambda: defaultdict(set)))
    for topology,matches in topodict.items():
        grouped_by_nodetypes = defaultdict(set)
        for match in matches:
            #Each match is an a-b-c triple. We can first group by types
            types = tuple( [nodes[m] for m in match ])
            grouped_by_nodetypes[types].add(match)
        for nts,ms in grouped_by_nodetypes.items():
            for m in ms:
                ns = [snode,m[0],m[1],m[2],tnode]
                #First types are sa, ab, bc, cta.
                etypes = [ edges[(ns[i],ns[i+1])] for i in range(4) ]
                for e_i,top_i in zip(e_indexes,topology):
                    #Don't need to pad because the edge existence is our top sort
                    if not top_i:
                        continue
                    etypes.append( edges[(ns[e_i[0]],ns[e_i[1]])] )
                etuple = tuple(etypes)
                grouped[topology][nts][etuple].add(m)
    indy_nodes = []
    for topology, g2 in grouped.items():
        for nts, g3 in g2.items():
            for etuple, g4 in g3.items():
                (a, b, c) = next(iter(g4))
                indy_nodes.append((a, b, c))
    return indy_nodes

def make_paths(nids,ids,edges,q=None):
    #nids = list(nodes.keys()) + ids # All the node_ids in this graph
    ugraph = networkx.Graph()
    tedges = [ (x,y,z) for x,y,z in edges if ( (x in nids) and (y in nids) )]
    ugraph.add_edges_from(tedges)
    #maybe by pulling out nodes we ended up with a graph in which our terminal nodes are unconnected.
    if not ugraph.has_node(ids[0]):
        return None
    if not ugraph.has_node(ids[1]):
        return None
    if q is None:
        paths = networkx.all_simple_paths(ugraph, source=ids[0], target=ids[1])
    else:
        paths = networkx.all_simple_paths(ugraph, source=ids[0], target=ids[1], cutoff=q+1)
    return paths

def makegraph(nodes,edges,ids,alabel,blabel):
    #Expects that edges is in the form of a list of triples (source_id, target_id, {'predicate':predicate})
    #First, we need to know that we want all these nodes.  Because of the way this thing is generated, it might contain lots of danglers
    # The simplest thing is to use the all simple paths approach, but for that, we need this to be undirected (ugh)
    ngraph = networkx.DiGraph()
    ngraph.add_edges_from(edges)
    #for edge in edges:
    #    print(edge)
    #for nid,label in nodes.items():
    #    print(nid,label)
    for nid, nl in nodes.items():
        ngraph.nodes[nid]['label'] = nl
    ngraph.nodes[ids[0]]['label'] = f'input_{alabel}'
    ngraph.nodes[ids[1]]['label'] = f'input_{alabel}'
    return ngraph

def compact_graph(g):
    same_neighbors = lambda u, v: (
        (list(g.predecessors(u)) == list(g.predecessors(v)))
        and
        (list(g.successors(u)) == list(g.successors(v)))
        and
        all([ g[p][u]['predicate'] == g[p][v]['predicate'] for p in g.predecessors(u) ])
        and
        all([ g[u][p]['predicate'] == g[v][p]['predicate'] for p in g.successors(u) ])
    )
    Q = networkx.quotient_graph(g, same_neighbors)
    return Q

def filter_nodes(internodes,edges,original_nodes,l):
    #Add all the edges into a grpah and use it to find any edges that are not on a simple path between a and b
    bgraph = networkx.Graph()
    bgraph.add_edges_from(edges)
    remove = [node for node, degree in bgraph.degree() if degree < 2]
    while len(remove) > 0:
        bgraph.remove_nodes_from(remove)
        remove = [node for node, degree in dict(bgraph.degree()).items() if degree < 2]
    print(f'original dehairing gives {len(bgraph.nodes())}')
    if not ( bgraph.has_node(original_nodes[0]) and bgraph.has_node(original_nodes[1])):
        return []
    paths = networkx.all_simple_paths(bgraph,source=original_nodes[0],target=original_nodes[1],cutoff=l+1)
    keepnodes = set()
    for path in paths:
        keepnodes.update(path)
    print(f'second dehairing gives {len(keepnodes)}')
    left_edges = [ (x,y,z) for x,y,z in edges if ((x in keepnodes) and (y in keepnodes))]
    g = networkx.DiGraph()
    g.add_edges_from(left_edges)
    q = compact_graph(g)
    keepnodes = [ next(iter(s)) for s in q.nodes() ]
    print(f'quotient graph leaves {len(keepnodes)}')
    return [ node for node in internodes if node in keepnodes ]

def get_direct_edges(all_edges,ab):
    a_to_b = [(x,y,z) for x,y,z in all_edges if ((x==ab[0]) and (y==ab[1]))]
    b_to_a = [(x,y,z) for x,y,z in all_edges if ((x==ab[1]) and (y==ab[0]))]
    return a_to_b + b_to_a

def node_combs(n3,n2,n1):
    kept = set()
    for p in n3:
        sp = frozenset(p)
        if sp in kept:
            continue
        kept.add(sp)
        yield sp
    for n2p in combinations(n2,2):
        sp = frozenset( n2p[0] + n2p[1])
        if len(sp) != 3:
            continue
        if sp in kept:
            continue
        kept.add(sp)
        yield sp
    for n2p in n2:
        for n1p in n1:
            sp = frozenset(n2p + n1p)
            if len(sp) != 3:
                continue
            if sp in kept:
                continue
            kept.add(sp)
            yield sp
    for n1p in combinations(n1,3):
        sp = frozenset( n1p[0] + n1p[1] + n1p[2])
        if len(sp) != 3:
            continue
        if sp in kept:
            continue
        kept.add(sp)
        yield sp