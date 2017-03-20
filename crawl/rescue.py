"""
Some one-off functions for repair of partially broken crawling jobs.
"""
import os
import sys
import time
import random
from collections import Counter
from collections import defaultdict
from itertools import islice
from crawl.walk import bbl2dirpath_short, bbl2dirpath
from crawl.logging import log
from crawl.util import mkdir_soft, make_bbl_dir
from crawl.decorators import timedsingle


def rescue_target(pulldir,stype,bbl):
    c = Counter()
    subdir = bbl2dirpath_short(bbl)
    extlist = ['pdf','txt','html']
    for ext in extlist:
        oldpath = "%s/%s/%d.%s" % (pulldir,subdir,bbl,ext)
        if os.path.exists(oldpath):
            log.info("PUSH oldpath = %s" % oldpath)
            newdir = make_bbl_dir(pulldir,bbl)
            newfile = "%s-%d.%s" % (stype,bbl,ext)
            newpath = "%s/%s" % (newdir,newfile)
            log.info("DEST newpath = %s" % newpath)
            os.rename(oldpath,newpath)
            c['push'] += 1
        else:
            log.info("SKIP oldpath = %s" % oldpath)
            c['skip'] += 1
    return c


@timedsingle
def rescue(pulldir,stype,targets):
    total = Counter()
    for n in targets:
        total += rescue_target(pulldir,stype,n)
    return total


def purge_target(pulldir,stype,bbl):
    c = Counter()
    subdir = bbl2dirpath(bbl)
    pathbase = "%s/%s/%s-%d" % (pulldir,subdir,stype,bbl)
    checkpath = "%s.html" % pathbase
    if os.path.exists(checkpath):
        badpath = "%s.txt" % pathbase
        if os.path.exists(badpath):
            os.remove(badpath)
            log.info("KILL badpath = %s" % badpath)
            c['kill'] += 1
        else:
            log.info("SKIP badpath = %s" % badpath)
            c['skip-txt'] += 1
    else:
        log.info("SKIP checkpath = %s" % checkpath)
        c['skip-html'] += 1
    return c


def rename_target(pulldir,stype,bbl):
    c = Counter()
    subdir = bbl2dirpath(bbl)
    pathbase = "%s/%s/%s-%d" % (pulldir,subdir,stype,bbl)
    oldpath = "%s.pdf" % pathbase
    newpath = "%s.html" % pathbase
    if os.path.exists(oldpath):
        log.info("MOVE oldpath = %s" % oldpath)
        os.rename(oldpath,newpath)
        c['move'] += 1
    else:
        log.info("SKIP oldpath = %s" % oldpath)
        c['skip'] += 1
    return c

@timedsingle
def movepdfs(pulldir,stype,targets):
    total = Counter()
    for n in targets:
        total += rename_target(pulldir,stype,n)
    return total



