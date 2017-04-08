import os
import random
from collections import defaultdict
from itertools import islice
import argparse
import ioany
from crawl.io import read_ints
from crawl.walk import analyze, bbl2dirpath
from crawl.util import  init_stash, save_list_like
from crawl.parse import parse
from crawl.decorators import timedsingle
from crawl.rescue import rescue, movepdfs
from crawl.logging import log


parser = argparse.ArgumentParser()
parser.add_argument("--spec", required=True, type=str, help="date spec")
parser.add_argument("--limit", required=False, type=int, help="limit")
parser.add_argument("--stash", required=False, type=str, help="stash directory", default="stash")
parser.add_argument("--random", required=False, action="store_true", help="randomize targets")
parser.add_argument("--targets", required=False, type=str, help="explicit targets list")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--check", action="store_true", help="check")
group.add_argument("--parse", action="store_true", help="parse")
group.add_argument("--rescue", action="store_true", help="rescue")
group.add_argument("--movepdfs", action="store_true", help="rescue")
# parser.add_argument("--loud", required=False, action="store_true", help="emit more data")
args = parser.parse_args()
log.info("args = %s" % args)
print("pid = %s" % os.getpid())

def init_targets(d,args):
    if args.targets:
        targetsfile = args.targets
    else:
        targetsfile = "%s/targets.txt" % d['meta']
    print("targets from '%s' .." % targetsfile)
    targets = list(read_ints(targetsfile))
    targets = refine(targets,args)
    print("that be %d targets." % len(targets))
    log.info("that be %d targets." % len(targets))
    return targets

def process_target(pulldir,stype,bbl):
    subdir = bbl2dirpath(bbl)
    fname = "%s-%d.pdf" % (stype,bbl)
    infile = "%s/%s/%s" % (pulldir,subdir,fname)
    log.info("infile = %s" % infile)
    if os.path.exists(infile):
        with open(infile,"rb") as f:
            return parse(f)
    else:
        return None

def expand(bbl,d):
   for section,kvdict in d.items():
       for name,attr in kvdict.items():
           yield bbl,section,name,attr

def traverse(pulldir,stype,targets):
    status = defaultdict(list)
    def _walkdir():
        for bbl in targets:
            log.debug("bbl = %s" % bbl)
            try:
                d = process_target(pulldir,stype,bbl)
            except Exception as e:
                log.info("FAIL %s = %s" % (bbl,e))
                log.exception(e)
                status['fail'] += [bbl]
                continue
            if d:
                log.info("GOOD %s" % bbl)
                status['good'] += [bbl]
                yield from expand(bbl,d)
            else:
                log.info("MISS %s" % bbl)
                status['miss'] += [bbl]
    walker = (_ for _ in _walkdir())
    return walker,status

@timedsingle
def process(d,stype,spec,targets):
    print("process ..")
    walker,status = traverse(d['pull'],stype,targets)
    outfile = "%s/tokens-%s.csv" % (d['output'],spec)
    print ("output to '%s' .." % outfile)
    header = ('bbl','section','name','attr')
    ioany.save_csv(outfile,walker,header=header)
    return status


def refine(targets,args):
    if args.random:
        random.shuffle(targets)
    if args.limit:
        targets = targets[:args.limit]
    return targets

d = init_stash(args)
targets = init_targets(d,args)


STYPE = 'SOA'
if args.check:
    seen,missing,delta = analyze(d['pull'],STYPE,targets)
    print("check'd in %.3f sec" % delta)
    save_list_like(d['data'],seen,'check-seen')
    save_list_like(d['data'],missing,'check-missing')
elif args.parse:
    print("parse ..")
    status,delta = process(d,STYPE,args.spec,targets)
    print("parse'd in %.3f sec" % delta)
    save_list_like(d['data'],status,'parse-status')
elif args.rescue:
    print("rescue ..")
    total,delta = rescue(d['pull'],STYPE,targets)
    print("rescued in %.3f sec" % delta)
    print("total = %s" % total)
    log.info("rescued in %.3f sec" % delta)
    log.info("total = %s" % total)
elif args.movepdfs:
    print("movepdfs..")
    total,delta = movepdfs(d['pull'],STYPE,targets)
    print("moved in %.3f sec" % delta)
    print("total = %s" % total)
    log.info("rescued in %.3f sec" % delta)
    log.info("total = %s" % total)
else:
    # Logically exluded by argparse
    raise ValueError("invalid state")
print("done.")


"""
elif args.convert:
    print("convert ..")
    status,delta = perform(d['pull'],STYPE,convert,limit=args.limit)
    print("convert'd in %.3f sec" % delta)
    save_list_like(d['data'],status,'convert-status')
"""

