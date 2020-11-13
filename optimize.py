import numpy as np
from collections import defaultdict
import os

from pymoo.algorithms.nsga2 import NSGA2
from pymoo.factory import get_crossover
from pymoo.optimize import minimize
from pymoo.model.mutation import Mutation
from pymoo.model.problem import Problem
from pymoo.model.sampling import Sampling

#Need these 2 for access to the config
import dask
import dask.distributed

from dask_jobqueue import SLURMCluster
from dask.distributed import Client

import sys

from datetime import datetime as dt

import logging
#logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

class DataProducer():
    hits_misses_hitgraphs = None
    def get_data(self,logger):
        indir = 'gene_disease'
        if self.hits_misses_hitgraphs is None:
            self.hits_misses_hitgraphs = self.read_data(indir,logger)
        return self.hits_misses_hitgraphs
    def read_data(self,indir,logger):
        print('reading data')
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
        #Let's filter out very small queries
        big_hits = {x:y for x,y in hits.items() if len(y) > 20}
        #We don't need to use up memory holding a bunch of unconnected graphs
        filtered_misses = { x:y for x,y in misses.items() if x in big_hits }
        hits = defaultdict(set,big_hits)
        misses = defaultdict(set,filtered_misses)
        print('Read the data',len(hits)) 
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
        maxbits = 10
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
            while now > maxbits:
                #This thing is too big, let's shutdown a number of these.  At least now - maxbits
                on_indexes = np.where(_X[i, :])[0]
                turnoff_indices = np.random.choice(on_indexes)
                _X[i, turnoff_indices] = np.logical_not(X[i,turnoff_indices])
                now = np.count_nonzero(_X[i, :])
        return _X.astype(np.bool)

class SubsetSelector(Problem):
    def __init__(self,client,hits_f,misses_f,nvars,**kwargs):
        self.client = client
        self.hits_f = hits_f
        self.misses_f = misses_f
        super().__init__(n_var=nvars, n_obj=3, n_constr=0, xl=0, xu=1, type_var=np.bool, elementwise_evaluation=False,**kwargs)

    def _evaluate(self, x, out, *args, **kwargs):
        #There are 3 outputs:
        # 1. negative of the number of unique pairs hit by these queries
        # 2. number of unique miss pairs for these queries
        # 3. Number of queries
        # These will be simultaneously minimized (why 1 is negatived)
        def fun(individual,hits,misses):
            hitgraphs = list(hits.keys())
            hitgraphs.sort()
            logger=logging.getLogger('distributed.worker')
            logger.info("Evaluating 1")
            start = dt.now()
            ongraphs = [hitgraphs[bitcount] for bitcount,bit in enumerate(individual) if bit ]
            qhits = set()
            qmisses = set()
            for g in ongraphs:
                qhits.update(hits[g])
                #scatter turned misses into a dumb dict
                if g in misses:
                    qmisses.update(misses[g])
            numq = len(ongraphs)
            end = dt.now()
            logger.info(f'Took {end-start}')
            return [-len(qhits),len(qmisses),numq]
        jobs = [self.client.submit(fun, individual, self.hits_f, self.misses_f) for individual in x]
        out["F"] = np.row_stack([job.result() for job in jobs])

    def single_evaluate(self, individual, out, *args, **kwargs):
        #There are 3 outputs:
        # 1. negative of the number of unique pairs hit by these queries
        # 2. number of unique miss pairs for these queries
        # 3. Number of queries
        # These will be simultaneously minimized (why 1 is negatived)
        ongraphs = [self.hitgraphs[bitcount] for bitcount,bit in enumerate(individual) if bit ]
        qhits = set()
        qmisses = set()
        for g in ongraphs:
            qhits.update(self.hits[g])
            qmisses.update(self.misses[g])
        numq = len(ongraphs)
        out["F"] = [-len(qhits),len(qmisses),numq]

algorithm = NSGA2(
    pop_size=200,
    n_offsprings=200,
    sampling=VariableBinarySampling(5),
    crossover=get_crossover("bin_hux"),
    mutation=SparseBitflipMutation(1),
    eliminate_duplicates=True
)

#To get around timeouts for scattering data
dask.config.set({"distributed.comm.timeouts.tcp":"120s"})
n_workers=20
cluster = SLURMCluster(cores=1,memory="48GB",walltime="24:00:00")
cluster.scale(n_workers)
client = Client(cluster)

#read_data is done on the workers too, but we do it here to write out the bit meanings
provider = DataProducer()
logger = logging.getLogger()
hits,misses = provider.get_data(logger)
hitgraphs = list(hits.keys())
hitgraphs.sort()
with open('graphs.bitstring','w') as outf:
    for g_id in hitgraphs:
        outf.write(f'{g_id}\n')

print('waiting for workers')
client.wait_for_workers(n_workers)
print('have workers')

#One annoying thing is that the scatter converts our nice default dicts into just dumb dicts
#another annoying thing is that you can say 
#hits_future = client.scatter(hits,broadcast)
#but in that case, hits_future is not infact a future.  Why? No idea.  Docs do not help.
print('scatter')
[hits_future,misses_future] = client.scatter([hits,misses],broadcast=True)
print('scattered')

n_vars = len(hitgraphs)

print('And begin')

res = minimize(SubsetSelector(client,hits_future,misses_future,n_vars),
               algorithm,
               ('n_gen', 200),
               verbose=True)

print('Time:', res.exec_time)

print("Best solution found: %s" % res.X.astype(np.int))
print("Function value: %s" % res.F)
print("Constraint violation: %s" % res.CV)

np.save('pareto_individuals',res.X.astype(np.int))
np.save('pareto_scores',res.F)
print(res.pop.get("X").shape)
np.save('final_individuals',res.pop.get("X").astype(np.int))
np.save('final_scores',res.pop.get("F"))
