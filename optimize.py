import numpy as np
from collections import defaultdict
import os

from pymoo.algorithms.nsga2 import NSGA2
from pymoo.factory import get_crossover
from pymoo.optimize import minimize
from pymoo.model.mutation import Mutation
from pymoo.model.problem import Problem
from pymoo.model.sampling import Sampling

def read_data(indir):
    cdir = f'{indir}_connected'
    udir = f'{indir}_unconnected'
    files = [ f'{cdir}/{f}' for f in os.listdir(cdir) ]
    #files += [ f'{udir}/{f}' for f in os.listdir(udir) ]
    hits = defaultdict( set )
    misses = defaultdict( set )
    n = 0
    for f in files[:10]:
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

class VariableBinarySampling(Sampling):

    def __init__(self,n_on) -> None:
        super().__init__()
        self.n_on = n_on

    def _do(self, problem, n_samples, **kwargs):
        f = self.n_on / problem.n_var
        val = np.random.random((n_samples, problem.n_var))
        X = (val < f).astype(np.bool)
        #print(np.count_nonzero(X))
        #for i in range(X.shape[0]):
        #    print('row',np.count_nonzero(X[i,:]))
        return X

class SparseBitflipMutation(Mutation):

    def __init__(self, base=None):
        super().__init__()
        self.base = base

    def _do(self, problem, X, **kwargs):
        # The idea is that we want a mutation operator that does not overall favor equal numbers of on and off
        # This is important for sparse bitstrings
        # So if there are T true bits, and F false bits, and t is the prob(True->False) and  f is the prob(False->True)
        # then we want f F = t T
        # And we want to control the total number of flips as t T = base (defaults to 1)
        # then t = base / T
        # f = (T / F) t
        if self.base is None:
            self.base = 1.0
        _X = np.full(X.shape, np.inf)
        for i in range(X.shape[0]):
            #_X[i, :] = X[i, :]
            is_true = X[i, :]
            is_false = (np.logical_not(is_true))
            n_false = np.count_nonzero(is_false)
            n_true = np.count_nonzero(is_true)
            p_false_to_true = self.base / n_false
            if n_true > 0:
                p_true_to_false = p_false_to_true * (n_false / n_true)
            else:
                p_true_to_false = 1.
            M = np.random.random(_X[i,:].shape)
            flip_true_to_false= (M < p_true_to_false) & is_true
            flip_false_to_true= (M < p_false_to_true) & is_false
            flip = flip_true_to_false | flip_false_to_true
            no_flip = np.logical_not(flip)
            _X[i,flip] = np.logical_not(X[i,flip])
            _X[i,no_flip] = X[i,no_flip]
            now = np.count_nonzero(_X[i, :])
        return _X.astype(np.bool)

class SubsetSelector(Problem):
    def __init__(self, typea, typeb):
        self.hits,self.misses = read_data(f'{typea}_{typeb}')
        self.hitgraphs = list(self.hits.keys())
        self.hitgraphs.sort()
        with open('graphs.bitstring','w') as outf:
            for g_id in self.hitgraphs:
                outf.write(f'{g_id}\n')
        super().__init__(n_var=len(self.hitgraphs), n_obj=3, n_constr=0, xl=0, xu=1, type_var=np.bool)

    def _evaluate(self, x, out, *args, **kwargs):
        #There are 3 outputs:
        # 1. negative of the number of unique pairs hit by these queries
        # 2. number of unique miss pairs for these queries
        # 3. Number of queries
        # These will be simultaneously minimized (why 1 is negatived)
        results = []
        for individual in x:
            ongraphs = [self.hitgraphs[bitcount] for bitcount,bit in enumerate(individual) if bit ]
            qhits = set()
            qmisses = set()
            for g in ongraphs:
                qhits.update(self.hits[g])
                qmisses.update(self.misses[g])
            numq = len(ongraphs)
            results.append( [-len(qhits),len(qmisses),numq])
        out["F"] = results

algorithm = NSGA2(
    pop_size=200,
    n_offsprings=200,
    sampling=VariableBinarySampling(5),
    crossover=get_crossover("bin_hux"),
    mutation=SparseBitflipMutation(1),
    eliminate_duplicates=True
)

res = minimize(SubsetSelector('gene','disease'),
               algorithm,
               ('n_gen', 2),
               save_history=True,
               verbose=True)

print("Best solution found: %s" % res.X.astype(np.int))
print("Function value: %s" % res.F)
print("Constraint violation: %s" % res.CV)

np.save('pareto_individuals',res.X.astype(np.int))
np.save('pareto_scores',res.F)
print(res.pop.get("X").shape)
np.save('final_individuals',res.pop.get("X").astype(np.int))
np.save('final_scores',res.pop.get("F"))

