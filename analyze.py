import argparse
import ast
import os
import json
import numpy as npy
from collections import defaultdict
import pandas as pd

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
            with open(f'{indir}/{f}') as inf:
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
    for ps in range(20):
        eps= is_pareto_efficient(points,return_mask = False)
        #surfaces.append( set([ frozenset(points[ep]) for ep in eps]) )
        for ep in eps:
            surfaces[ (frozenset(points[ep])) ] = ps+1
        points = npy.delete(points, eps, axis=0)
    with open(f'{indir}/pareto.txt','w') as outf:
        outf.write('n_connected\tn_unconnected\tnum_nodes\tnum_edges\tgraphs\tPareto\n')
        for p,parts in gc.items():
            for (nn,ne),gs in parts.items():
                outf.write(f'{p[0]}\t{p[1]}\t{nn}\t{ne}\t{gs}\t')
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
                gs = ast.literal_eval(x[4])
                for g in gs:
                    gres.append( (surface,nc,nu,numnodes,numedges,g) )
                    graphs_hashes.add(g)
    gres.sort()
    #Have to dig through everything to find these bozos
    files = os.listdir(indir)
    graphs = {}
    for f in files:
        if f.startswith('connected') and f.endswith('graphs'):
            print(f)
            with open(f'{indir}/{f}','r') as inf:
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
        outf.write('surface\tn_connected\tn_unconnected\tnum_nodes\tnum_edges\tgraph\n')
        for s,n,m,nn,ne,g in gres:
            trapig = convert_graph(graphs[g])
            outf.write(f'{s}\t{n}\t{-m}\t{nn}\t{ne}\t{trapig}\n')

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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', action='store', dest='input_directory', help='input directory')
    results = parser.parse_args()
    #analyze(results.input_directory)
    #analyze('gene_disease')
    #examine('gene_disease')
    draw('gene_disease')
