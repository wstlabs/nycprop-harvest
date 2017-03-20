import os
import random
from collections import defaultdict
from itertools import islice
import argparse
import ioany
from crawl.io import read_ints
from crawl.walk import analyze, perform, bbl2dirpath
# from crawl.logging import log
from crawl.pdfutil import textify
from crawl.util import  init_stash
from crawl.parse import parse
from crawl.decorators import timedsingle


parser = argparse.ArgumentParser()
parser.add_argument("--spec", required=True, type=str, help="date spec")
parser.add_argument("--bbl", required=True, type=int, help="bbl")
parser.add_argument("--stash", required=False, type=str, help="stash directory", default="stash")
args = parser.parse_args()
# log.info("args = %s" % args)

def process_target(pulldir,stype,bbl):
    subdir = bbl2dirpath(bbl)
    fname = "%s-%d.pdf" % (stype,bbl)
    infile = "%s/%s/%s" % (pulldir,subdir,fname)
    # log.info("infile = %s" % infile)
    with open(infile,"rb") as f:
        for line in textify(f):
            print(line)

STYPE = 'SOA'
d = init_stash(args)
process_target(d['pull'],STYPE,args.bbl)

