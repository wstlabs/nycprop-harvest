import os
import sys
import time
from crawl.sift import collect
from crawl.util import init_stash, split_spec
from crawl.decorators import timedmulti
from collections import defaultdict
from itertools import islice
import argparse
import ioany

parser = argparse.ArgumentParser()
parser.add_argument("--spec", required=True, type=str, help="date spec")
parser.add_argument("--stash", required=False, type=str, help="stash directory", default="stash")
parser.add_argument("--limit", required=False, type=int, help="limit")
args = parser.parse_args()
LOUD = False


d = init_stash(args)
infile = "%s/tokens-%s.csv" % (d['output'],args.spec)
outfile = "%s/values-%s.csv" % (d['output'],args.spec)
print("infile = %s" % infile)
tokens = ioany.read_csv(infile,types=(int,str,str,str)).rows()
pairs = collect(tokens)
pairs = islice(pairs,args.limit)
spec = args.spec


@timedmulti
def harvest(pairs,spec):
    year,quarter = split_spec(spec)
    seen = defaultdict(set)
    def walker():
        for bbl,r in pairs:
            if bbl in seen['valid']:
                raise ValueError("duplicate BBL %s" % bbl)
            seen['valid'].add(bbl)
            htype     = r['general']['htype']
            taxclass  = r['general']['tax-class']
            amount    = r['general']['total-amount-due']
            estimated = r['general']['estimated-market-value']
            unitcount = r['stabilization']['unitcount']
            _421a = 1 if r['abatements']['421a'] else ''
            _J51 = 1 if r['abatements']['J51'] else ''
            yield bbl,year,quarter,htype,taxclass,unitcount,estimated,amount,_421a,_J51
    values = (_ for _ in walker())
    return values,seen


print("harvesting to '%s' .." % outfile)
header = ('bbl','year','quarter','htype','taxclass','unitcount','estimated','amount','_421a','_J51')
values,seen,delta = harvest(pairs,spec)
ioany.save_csv(outfile,stream=values,header=header)
print("harvest'd in %.3f sec" % delta)



"""
def soft_null(x):
    return x if x else 'NULL'
"""

