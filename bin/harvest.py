import os
import sys
import time
from crawl.sift import collect
from crawl.util import init_stash, expand_date_spec
from crawl.decorators import timedmulti
from collections import defaultdict
from itertools import islice
import argparse
import ioany

parser = argparse.ArgumentParser()
parser.add_argument("--spec", required=False, type=str, help="date spec")
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
ACTIVEDATE = expand_date_spec(args.spec)

@timedmulti
def harvest(pairs):
    seen = defaultdict(set)
    def walker():
        for bbl,r in pairs:
            seen['valid'].add(bbl)
            s = r['statement']
            # print("s[%d] = %s" % (bbl,s))
            yield bbl,s['class'],s['amount'],ACTIVEDATE
    values = (_ for _ in walker())
    return values,seen

def crank(pairs):
    for bbl,r in pairs:
        s = r['statement']
        yield bbl,s['class'],s['amount']


# stream = crank(tokens)
# for t in islice(stream,args.limit):
#    print(t)

print("harvesting to '%s' .." % outfile)
header = ('bbl','htype','balance','duedate')
values,seen,delta = harvest(pairs)
ioany.save_csv(outfile,stream=values,header=header)
print("harvest'd in %.3f sec" % delta)


sys.exit(1)


def harvest(stream):
    seen = defaultdict(set)
    def select(stream):
        for bbl,r in collect(stream):
            r = normalize(r)
            r = dictify(r)
            seen['valid'].add(bbl)
            klass = r['header-class']['class']
            if klass == 'class-1':
                balance = r['total-amount-due']['balance']
                klass = 1
            elif klass == 'class-2':
                balance = r['header-amount-due']['amount']
                klass = 2
            else:
                raise ValueError("no tax class for bbl = %s" % bbl)
            duedate = r['total-amount-due']['duedate']
            taxclass = r['tax-class']['taxclass']
            yield bbl,klass,taxclass,duedate,balance
    return select,seen


def dispatch(stream,args):
    if args.limit:
        stream = islice(stream,args.limit)
    return harvest(stream)



print("okay ..")
infile = "%s/tokens.csv" % args.indir
stream = ioany.read_csv(infile,types=(int,str,str,str)).rows()
select,seen = dispatch(stream,args)
values = select(stream)

t0 = time.time()
outfile = "%s/harvest.csv" % args.indir
print("harvesting to '%s' .." % outfile)
header = ('bbl','htype','taxclass','duedate','balance')
ioany.save_csv(outfile,stream=values,header=header)
delta = time.time() - t0
print("harvest'd in %.3f sec" % delta)

print("saving ..")
tags = sorted(seen.keys())
for k in tags:
    count = len(seen[k])
    outfile = "%s/final-%s.txt" % (args.indir,k)
    print("%d %s BBLs to %s .." % (count,k,outfile))
    ioany.save_lines(outfile,sorted(str(_) for _ in seen[k]))
print("done.")



