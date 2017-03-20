import os
import sys
import time
from collections import defaultdict
from itertools import islice
import ioany
from crawl.decorators import timedsingle, timedmulti
from crawl.logging import log
from crawl.util import split_bbl

def bbl2dirpath_short(bbl):
    boro,block,lot = split_bbl(bbl)
    boro = str(boro)
    block = "%.5d" % block
    lot = "%.4d" % lot
    return "/".join([boro,block])

def bbl2dirpath(bbl):
    boro,block,lot = split_bbl(bbl)
    boro = str(boro)
    block = "%.5d" % block
    lot = "%.4d" % lot
    return "/".join([boro,block,lot])

def dirpath2bbl(dirpath):
    """Parses a BBL from a dirname, e.g. '1/00015/0022/' -> 1000150022"""
    topdir,boro,block,lot = dirpath.split("/")
    return int(boro) * 1000000000 + int(block) * 10000 + int(lot)

def fname2bbl(fname):
    root,ext = os.path.splitext(fname)
    stype,bblstring = root.split("-")
    return int(bblstring)

def fname2ext(fname):
    root,ext = os.path.splitext(fname)
    if ext is None:
        return ext
    return ext[1:]

def split_fname(fname):
    root,ext = os.path.splitext(fname)
    stype,bblstring = root.split("-")
    bbl = int(bblstring)
    ext = ext[1:] if ext is not None else None
    return stype,bbl,ext

def walkdir(dirname,stype,extlist):
    """Yields all files under a given directory root matching the given extension"""
    tuples = os.walk(dirname)
    extset = set(".%s" % k for k in extlist)
    for dirpath,dirname,filenames in tuples:
        if len(filenames) > 0:
            for fname in filenames:
                if not fname.startswith(stype):
                    continue
                _root,_ext = os.path.splitext(fname)
                if _ext in extset:
                    yield (dirpath,fname)


def inventory(dirname,stype):
    """Make a quick inventory of which BBLs we have .pdf/.txt files for."""
    log.info('..')
    seen = defaultdict(set)
    extset = ('pdf','txt','html')
    pairs = walkdir(dirname,stype,extset)
    for dirpath,fname in pairs:
        stype,bbl,ext = split_fname(fname)
        seen[ext].add(bbl)
    return seen

@timedmulti
def analyze(dirname,stype,targets):
    missing = {}
    seen = inventory(dirname,stype)
    targets  = set(targets)
    crawled = seen['pdf'].union(seen['html'])
    missing['any'] = sorted(targets - crawled)
    missing['pdf'] = sorted(targets - seen['pdf'])
    missing['txt'] = sorted(seen['pdf'] - seen['txt'])
    return seen,missing


@timedsingle
def perform(dirname,stype,convert=None,limit=None):
    log.info("limit = %s" % limit)
    log.info("dirname = %s" % dirname)
    status = defaultdict(list)
    pairs = walkdir(dirname,stype,['pdf'])
    pairs = islice(pairs,limit)
    for dirpath,fname in pairs:
        _stype,bbl,ext = split_fname(fname)
        if _stype != stype:
            raise ValueError("stype mismatch")
        log.debug("stype,bbl = %s,%d .." % (_stype,bbl))
        status['seen'] += [bbl]
        infile  = "%s/%s" % (dirpath,fname)
        outfile = "%s/%s-%d.txt" % (dirpath,stype,bbl)
        if os.path.exists(outfile):
            log.info("SKIP %d" % bbl)
            status['skip'] += [bbl]
            continue
        try:
            convert(infile,outfile)
            log.info("GOOD %d" % bbl)
            status['good'] += [bbl]
        except Exception as e:
            log.info("FAIL %d %s" % (bbl,e))
            log.exception(e)
            status['fail'] += [bbl]
    return status


def __visit(pairs):
    seen = set()
    stats = defaultdict(int)
    missing = defaultdict(list)
    def walker(pairs):
        for dirpath,fname in pairs:
            bbl = int(basename(fname))
            infile = "%s/%s" % (dirpath,fname)
            if LOUD:
                print("::",infile)
            kvdict = tokenize_fname(fname)
            yield bbl,'newfile',kvdict
            lines = ioany.slurp_lines(infile)
            d = analyze(lines)
            for label,kvdicts in d.items():
                if len(kvdicts):
                    yield from ((bbl,label,_) for _ in kvdicts)
                else:
                    missing[label] += [infile]
            stats['count'] += 1
            seen.add(bbl)
    return walker,missing,stats,seen



def expand(walker,pairs):
    for bbl,label,kvdict in walker(pairs):
        for k,v in kvdict.items():
            yield bbl,label,k,v

def execute(f,walker,pairs):
    f.write("bbl,section,attribute,value" + "\n")
    for bbl,label,k,v in expand(walker,pairs):
        blurb = ",".join([str(bbl),label,k,str(v)])
        f.write(blurb + "\n")

def stroll(f,pairs):
    walker,missing,stats,seen = visit(pairs)
    execute(f,walker,pairs)
    return missing,stats,seen

def exhaust(walker,pairs):
    for bbl,label,k,v in expand(walker,pairs):
        pass

def profile(pairs):
    walker,missing,stats,seen = visit(pairs)
    exhaust(walker,pairs)
    return missing,stats,seen

def _dispatch(pairs,args):
    limitpairs = islice(pairs,args.limit)
    if args.profile:
        return profile(limitpairs)
    else:
        outfile = "%s/tokens.csv" % args.outdir
        print("tokens to %s .." % outfile)
        with open(outfile,"wt") as f:
            return stroll(f,limitpairs)

def dispatch(pairs,args):
    t0 = time.time()
    missing,stats,seen = _dispatch(pairs,args)
    delta = time.time() - t0
    return missing,stats,seen,delta

