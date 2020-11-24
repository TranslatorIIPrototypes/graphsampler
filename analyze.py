import argparse
import ast
import os
import math
import json
import numpy as npy
from collections import defaultdict
import pandas as pd
import networkx
from networkx.readwrite import json_graph

def analyze(indir):
    cdir = f'{indir}_connected'
    udir = f'{indir}_unconnected'
    files = [ f'{cdir}/{f}' for f in os.listdir(cdir) ]
    files += [ f'{udir}/{f}' for f in os.listdir(udir) ]
    graphcounts = defaultdict( lambda: defaultdict(set) )
    predcounts = defaultdict( set )
    upairs = set()
    cpairs = set()
    n = 0
    nodecount = {}
    edgecount = {}
    for f in files:
        if f.endswith('counts'):
            n += 1
            print(n)
            with open(f) as inf:
                for line in inf:
                    x = line[:-1].split('\t')
                    graph = x[0]
                    aid = x[1]
                    atype = x[2]
                    bid = x[3]
                    btype = x[4]
                    ab = x[5]
                    ba = x[6]
                    nodecount[graph] = int(x[7])
                    edgecount[graph] = int(x[8])
                    nids = frozenset([aid,bid])
                    if (ab == '') and (ba == ''):
                        graphcounts[graph]["none"].add(nids)
                        predcounts["none"].add(nids)
                        upairs.add(nids)
                    if not ab == '':
                        graphcounts[graph][ab+"->"].add(nids)
                        predcounts[ab+"->"].add(nids)
                        cpairs.add(nids)
                    if not ba == '':
                        graphcounts[graph][ba+"<-"].add(nids)
                        predcounts[ba+"<-"].add(nids)
                        cpairs.add(nids)
    print("Unconnected Pairs",len(upairs))
    print("Connected Pairs",len(cpairs))
    gc = {}
    for g,pc in graphcounts.items():
        if g not in gc:
            gc[g] = {'predicates':{},'num_nodes': nodecount[g],'num_edges':edgecount[g] }
        for p in pc:
            gc[g]['predicates'][p] = len(graphcounts[g][p])
    pc = {}
    for p,nids in predcounts.items():
        pc[p] = len(nids)
    with open(f'{indir}/aggregated.json','w') as outf:
        json.dump(gc,outf,indent=4)
    with open(f'{indir}/predicates.json','w') as outf:
        json.dump(pc,outf,indent=4)

def examine(indir):
    with open(f'{indir}/aggregated.json','r') as inf:
        r = json.load(inf)
    gc = defaultdict(lambda: defaultdict(list))
    for g in r:
        n_none = 0
        n_some = 0
        nn = r[g]['num_nodes']
        ne = r[g]['num_edges']
        for p,np in r[g]['predicates'].items():
            if p == 'none':
                n_none += np
            else:
                n_some += np
        k = (n_some,-n_none)
        gc[k][(nn,ne)].append(g)
    points = npy.array(list(gc.keys()))
    surfaces = {}
    #for ps in range(20):
    ps = 0
    while len(points) > 0:
        eps= is_pareto_efficient(points,return_mask = False)
        #surfaces.append( set([ frozenset(points[ep]) for ep in eps]) )
        for ep in eps:
            surfaces[ (frozenset(points[ep])) ] = ps+1
        points = npy.delete(points, eps, axis=0)
        ps += 1
    with open(f'{indir}/pareto.txt','w') as outf:
        outf.write('n_connected\tn_unconnected\tnum_nodes\tnum_edges\tgraphs\tPareto\n')
        for p,parts in gc.items():
            for (nn,ne),gs in parts.items():
                for g in gs:
                    outf.write(f'{p[0]}\t{p[1]}\t{nn}\t{ne}\t{g}\t')
                    if frozenset(p) in surfaces:
                        outf.write(f'{surfaces[frozenset(p)]}\n')
                    else:
                        outf.write('0\n')

def draw(indir):
    graphs_hashes = set()
    gres = []
    with open(f'{indir}/pareto.txt','r') as inf:
        h = inf.readline()
        for line in inf:
            x = line.strip().split('\t')
            surface = int(x[-1])
            if surface > 0:
                nc = int(x[0])
                nu = int(x[1])
                numnodes=int(x[2])
                numedges=int(x[3])
                g = x[4]
                #gs = ast.literal_eval(x[4])
                #for g in gs:
                gres.append( (surface,nc,nu,numnodes,numedges,g) )
                graphs_hashes.add(g)
    gres.sort()
    #Have to dig through everything to find these bozos
    files = os.listdir(f'{indir}_connected')
    graphs = {}
    for f in files:
        if f.startswith('connected') and f.endswith('graphs'):
            print(f)
            with open(f'{indir}_connected/{f}','r') as inf:
                for line in inf:
                    x = json.loads(line.strip())
                    h = x['graph']['hash']
                    if not h in graphs_hashes:
                        continue
                    else:
                        graphs[h] = x
                        graphs_hashes.remove(h)
                        print(len(graphs_hashes))
                        if len(graphs_hashes) == 0:
                            break
        if len(graphs_hashes) == 0:
            break
    print('done')
    with open(f'{indir}/paretographs','w') as outf:
        outf.write('surface\tn_connected\tn_unconnected\tnum_nodes\tnum_edges\tgraphid\tgraph\n')
        for s,n,m,nn,ne,g in gres:
            try:
                trapig = convert_graph(graphs[g])
                outf.write(f'{s}\t{n}\t{-m}\t{nn}\t{ne}\t{g}\t{trapig}\n')
            except KeyError:
                #this can happen if the graph is only in the unconnected list. Not worried about those ones
                print(g)

def convert_graph(g):
    """Takes a deserialied networkx graph and turns it into a reasonerapi query string"""
    tg = {'nodes':[], 'edges':[]}
    nn = 0
    first = True
    nodemap = {}
    for node in g['nodes']:
        tnode = {}
        if node['label'].startswith('input'):
            tnode['id'] = node['label']
            tnode['type'] = '_'.join(node['label'].split('_')[1:])
            prefix = node['id'].split('_')[0]
            if first:
                tnode['curie'] = f'{prefix}:XXXXXXX'
            else:
                tnode['curie'] = f'{prefix}:YYYYYYY'
        else:
            tnode['id'] = f'n{nn}'
            nn += 1
            tnode['type'] = node['label']
        nodemap[node['id']] = tnode['id']
        tg['nodes'].append(tnode)
    for edgecount,edge in enumerate(g['links']):
        tedge = {'id': f'edge_{edgecount}'}
        tedge['source_id'] = nodemap[edge['source']]
        tedge['target_id'] = nodemap[edge['target']]
        tedge['type'] = edge['predicate']
        tg['edges'].append(tedge)
    return  json.dumps(tg)


#Adapted from: https://stackoverflow.com/questions/32791911/fast-calculation-of-pareto-front-in-python
def is_pareto_efficient(costs, return_mask = True):
    """
    Find the pareto-efficient points
    :param costs: An (n_points, n_costs) array
    :param return_mask: True to return a mask
    :return: An array of indices of pareto-efficient points.
        If return_mask is True, this will be an (n_points, ) boolean array
        Otherwise it will be a (n_efficient_points, ) integer array of indices.
    """
    is_efficient = npy.arange(costs.shape[0])
    n_points = costs.shape[0]
    next_point_index = 0  # Next index in the is_efficient array to search for
    while next_point_index<len(costs):
        nondominated_point_mask = npy.any(costs>costs[next_point_index], axis=1)
        nondominated_point_mask[next_point_index] = True
        is_efficient = is_efficient[nondominated_point_mask]  # Remove dominated points
        costs = costs[nondominated_point_mask]
        next_point_index = npy.sum(nondominated_point_mask[:next_point_index])+1
    if return_mask:
        is_efficient_mask = npy.zeros(n_points, dtype = bool)
        is_efficient_mask[is_efficient] = True
        return is_efficient_mask
    else:
        return is_efficient

def optimal_per_pair(indir):
    """We have a bunch of connected ones. For each connected pair, what's the best (most specific) query we could have used?
    The question here is - are there pairs that just require an unspecific query to get them?  That's what is suggested
    by the GA optimizer"""
    with open(f'{indir}/aggregated.json','r') as inf:
        r = json.load(inf)
    gc = {}
    for g in r:
        n_none = 0
        n_some = 0
        for p,np in r[g]['predicates'].items():
            if p == 'none':
                n_none += np
            else:
                n_some += np
        f = n_some / (n_some + n_none)
        k = {'some':n_some,'none':n_none, 'precision':f}
        gc[g] = k
    cdir = f'{indir}_connected'
    files = [f'{cdir}/{f}' for f in os.listdir(cdir)]
    best_precision = defaultdict(float)
    best_none = defaultdict(lambda : 1000000)
    for f in files:
        if f.endswith('counts'):
            with open(f) as inf:
                for line in inf:
                    x = line[:-1].split('\t')
                    graph = x[0]
                    pair=(x[1],x[3])
                    if gc[graph]['some'] <= 5:
                        continue
                    best_precision[pair] = max( [best_precision[pair], gc[graph]['precision']])
                    best_none[pair] = min( [best_none[pair], gc[graph]['none']])
    with open(f'{indir}/best.txt','w') as outf:
        outf.write('pair\tBestPrecision\tLeastMisses\n')
        for p in best_precision:
            outf.write(f'{p}\t{best_precision[p]}\t{best_none[p]}\n')

def depredicate(indir,outdir):
    hash_to_hash = {} #dictionary tracking hashes with predicates to those without predicates
    for connection in ['connected','unconnected']:
        print(connection)
        idir = f'{indir}_{connection}'
        odir = f'{outdir}_{connection}'
        files = [ f'{idir}/{f}' for f in os.listdir(idir) ]
        outgraph_hashes = set()
        if not os.path.exists(odir):
            os.mkdir(odir)
        with open(f'{odir}/all.graphs','w') as outgraphs:
            for f in files:
                if f.endswith('graphs'):
                    with open(f, 'r') as inf:
                        for line in inf:
                            x = json.loads(line.strip())
                            input_hash = x['graph']['hash']
                            if input_hash not in hash_to_hash:
                                nxg = json_graph.node_link_graph(x)
                                #Take the predicates off
                                for (n1, n2, d) in nxg.edges(data=True):
                                    d.clear()
                                outg = nxg.to_undirected()
                                del outg.graph['hash']
                                output_hash = networkx.weisfeiler_lehman_graph_hash(outg, node_attr='label', iterations=3, digest_size=16)
                                hash_to_hash[input_hash] = output_hash
                                if not output_hash in outgraph_hashes:
                                    outgraph_hashes.add(output_hash)
                                    outg.graph['hash'] = output_hash
                                    outgraphs.write(json.dumps(networkx.json_graph.node_link_data(outg)))
                                    outgraphs.write('\n')
    for connection in ['connected','unconnected']:
        print(connection)
        idir = f'{indir}_{connection}'
        odir = f'{outdir}_{connection}'
        files = [ f'{idir}/{f}' for f in os.listdir(idir) ]
        nf = 0
        nl = 0
        nw = 0
        with open(f'{odir}/all.counts', 'w') as outcounts:
            for f in files:
                if f.endswith('counts'):
                    nf += 1
                    with open(f, 'r') as inf:
                        for line in inf:
                            nl += 1
                            x = line.split('\t')
                            if x[0] not in hash_to_hash:
                                print(x[0])
                                continue
                            newhash = hash_to_hash[x[0]]
                            x[0] = newhash
                            outcounts.write('\t'.join(x))
                            nw += 1
        print(f'Read {nl} lines from {nf} files and wrote {nw}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', action='store', dest='input_directory', help='input directory')
    results = parser.parse_args()
    #analyze(results.input_directory)
    #analyze('gene_disease')
    #examine('gene_disease')
    #draw('gene_disease')
    #optimal_per_pair('gene_disease')
    depredicate('gene_disease','gene_disease_nopreds')
