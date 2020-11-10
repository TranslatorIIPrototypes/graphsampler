import numpy as np
from collections import defaultdict
import os

from pymoo.algorithms.nsga2 import NSGA2
from pymoo.factory import get_crossover, get_mutation, get_sampling
from pymoo.optimize import minimize
from pymoo.model.problem import Problem

def read_data(indir):
    cdir = f'{indir}_connected'
    udir = f'{indir}_unconnected'
    files = [ f'{cdir}/{f}' for f in os.listdir(cdir) ]
    files += [ f'{udir}/{f}' for f in os.listdir(udir) ]
    hits = defaultdict( set )
    misses = defaultdict( set )
    n = 0
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
                    nids = frozenset([aid,bid])
                    if (ab == '') and (ba == ''):
                        misses[graph].add(nids)
                    else:
                        hits[graph].add(nids)
    return hits,misses

class MultiObjectiveKnapsack(Problem):
    def __init__(self, n_items):
        super().__init__(n_var=n_items, n_obj=1, n_constr=1, xl=0, xu=1, type_var=np.bool)

        self.W = np.random.randint(1, 100, size=n_items)
        self.P = np.random.randint(1, 100, size=n_items)
        self.C = int(np.sum(self.W) / 10)

    def _evaluate(self, x, out, *args, **kwargs):
        f1 = - np.sum(self.P * x, axis=1)
        f2 = np.sum(x, axis=1)

        out["F"] = np.column_stack([f1, f2])
        out["G"] = (np.sum(self.W * x, axis=1) - self.C)

class SubsetSelector(Problem):
    def __init__(self, typea, typeb):
        self.hits,self.misses = read_data(f'{typea}_{typeb}')
        self.hitgraphs = list(self.hits.keys())
        self.hitgraphs.sort()
        super().__init__(n_var=len(self.hitgraphs), n_obj=3, n_constr=0, xl=0, xu=1, type_var=np.bool)

    def _evaluate(self, x, out, *args, **kwargs):
        #There are 3 outputs:
        # 1. negative of the number of unique pairs hit by these queries
        # 2. number of unique miss pairs for these queries
        # 3. Number of queries
        # These will be simultaneously minimized (why 1 is negatived)
        ongraphs = [self.hitgraphs[xi] for xi in x if xi ]
        f1 = - np.sum(self.P * x, axis=1)
        qhits = set()
        qmisses = set()
        for g in ongraphs:
            qhits.update(self.hits[g])
            qmisses.update(self.misses[g])
        numq = len(ongraphs)

        out["F"] = np.column_stack([-len(qhits), len(qmisses), numq])

algorithm = NSGA2(
    pop_size=200,
    #n_offsprings=10,
    sampling=get_sampling("bin_random"),
    crossover=get_crossover("bin_hux"),
    mutation=get_mutation("bin_bitflip"),
    eliminate_duplicates=True
)

res = minimize(SubsetSelector('gene','disease'),
               algorithm,
               ('n_gen', 100),
               verbose=True)

print("Best solution found: %s" % res.X.astype(np.int))
print("Function value: %s" % res.F)
print("Constraint violation: %s" % res.CV)
