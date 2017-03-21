import os
import sys
import time
from itertools import islice
from collections import defaultdict
import argparse
import ioany
from crawl.agent import Agent
from crawl.util import init_stash, make_bbl_dir
from crawl.logging import log

# A mapping between canonical billing periods and their expected publish dates. 
schedule = {
    '2016Q3': '20160603',
    '2016Q4': '20161118',
    '2017Q2': '20170224',
}


parser = argparse.ArgumentParser()
parser.add_argument("--spec", required=True, type=str, help="date spec of the form YYYYQN")
parser.add_argument("--stash", required=False, type=str, help="stash directory", default="stash")
parser.add_argument("--bounds", required=False, type=str, help="range tuple of the form start:limit")
parser.add_argument("--targets", required=False, type=str, help="target list")
parser.add_argument("--pace", required=False, type=str, help="tuple of the form N:k")
args = parser.parse_args()
log.info("args = %s" % args)

def save_pdf(path,content):
    with open(path,"wb") as f:
        f.write(content)

def process(pulldir,stype,pubdate,bbl):
    agent = Agent()
    dirpath = make_bbl_dir(pulldir,bbl)
    outfile = "%s/%s-%s.pdf" % (dirpath,stype,bbl)
    if os.path.exists(outfile):
        log.info("SKIP %s" % bbl)
        return 'skip'
    log.info("search %s .." % bbl)
    r = agent.search(bbl)
    if r.status_code != 200:
        log.info("FAIL %s" % bbl)
        return 'fail'
    log.info("grab %s .." % bbl)
    r = agent.grab(bbl,pubdate,stype)
    ctype = r.headers.get('Content-Type')
    log.info("grab.content-type %s = %s" % (bbl,ctype))
    if r.status_code != 200:
        log.info("FAIL %s" % bbl)
        return 'fail'
    if ctype is None:
        log.info("ERROR %s - bad content type '%s'" % (bbl,ctype))
        return 'error'
    if ctype.startswith('application/pdf'):
        log.info("GOOD %s - pdf" % bbl)
        save_pdf(outfile,r.content)
        return 'good'
    elif ctype.startswith('text/html'):
        log.info("MISS %s - html" % bbl)
        outfile = "%s/%s-%s.html" % (dirpath,stype,bbl)
        ioany.save_lines(outfile,r.text)
        return 'miss'
    else:
        # We got some completely unexpected content type
        log.info("ERROR %s - bad content type '%s'" % (bbl,ctype))
        return 'error'

def parsetup(s):
    a,b = s.split(":")
    a = None if a == '' else int(a)
    b = None if b == '' else int(b)
    return a,b

def modslice(sequence,n,k):
    for i,x in enumerate(sequence):
        if i % n == k:
            yield x

def refine(targets,args):
    """Create a refined list of targets, according to argument flags."""
    bounds = parsetup(args.bounds) if args.bounds else None
    if bounds:
        i,j = bounds
        log.info("that be %d raw targets; restricting by range .." % len(targets))
        targets = targets[i:j]
    pace = parsetup(args.pace) if args.pace else None
    if pace:
        log.info("that be %d raw targets; restricting by pace .." % len(targets))
        targets = list(modslice(targets,*pace))
    log.info("that be %d targets" % len(targets))
    return targets

def dispatch(pulldir,spec,targets):
    pubdate = schedule.get(spec)
    if pubdate is None:
        raise ValueError("invalid query spec '%s'" % spec)
    x = defaultdict(list)
    for n in targets:
        stat = process(pulldir,'SOA',pubdate,n)
        x[stat] += [n]
    return x

d = init_stash(args)
log.info("stash = %s" % args.stash)
infile = args.targets if args.targets else "%s/targets.txt" % d['meta']
targets = [int(_) for _ in ioany.read_lines(infile)]
targets = refine(targets,args)


print("let's go (pid = %s)" % os.getpid())
log.info("let's go..")
t0 = time.time()
x = dispatch(d['pull'],args.spec,targets)
delta = time.time() - t0
tally = {k:len(v) for k,v in x.items()}
print("done in %.3f sec" % delta)
print("tally = %s" % tally)
log.info("done in %.3f sec" % delta)
log.info("tally = %s" % tally)

log.info("done.")

