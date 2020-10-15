import argparse
import os
import json
from collections import defaultdict

def analyze(indir):
    files = os.listdir(indir)
    graphcounts = defaultdict( lambda: defaultdict(set) )
    predcounts = defaultdict( set )
    for f in files:
        if f.endswith('counts'):
            print(f, len(graphcounts))
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
                    nids = frozenset([aid,bid])
                    if (ab == '') and (ba == ''):
                        graphcounts[graph]["none"].add(nids)
                        predcounts["none"].add(nids)
                    if not ab == '':
                        graphcounts[graph][ab+"->"].add(nids)
                        predcounts[ab+"->"].add(nids)
                    if not ba == '':
                        graphcounts[graph][ba+"<-"].add(nids)
                        predcounts[ba+"<-"].add(nids)
    gc = {}
    for g,pc in graphcounts.items():
        if g not in gc:
            gc[g] = {}
        if len(pc) > 1:
            if 'none' in pc:
                print(list(pc.keys()))
        for p in pc:
            gc[g][p] = len(graphcounts[g][p])
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
    gc = []
    for g in r:
        n = 0
        for p,np in r[g].items():
            if (len(r[g]) == 1) and p == 'none':
                continue
            n += np
        if n > 0:
            gc.append( (n,g) )
    gc.sort()
    print(gc[:5])
    print('---')
    print(gc[-5:])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', action='store', dest='input_directory', help='input directory')
    results = parser.parse_args()
    #analyze(results.input_directory)
    #analyze('gene_disease')
    examine('gene_disease')
