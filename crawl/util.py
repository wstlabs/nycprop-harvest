import re
import os
from collections import OrderedDict
import ioany

specpat = re.compile('^\d{4}Q\d$')
def is_valid_date_spec(s):
    return bool(re.match(specpat,s))

bblpat = re.compile('^(\d)(\d{5})(\d{4})$')
def split_bbl(bbl):
    s = str(bbl)
    m = re.match(bblpat,s)
    if m:
        boro = int(m.group(1))
        block = int(m.group(2))
        lot = int(m.group(3))
        return boro,block,lot
    else:
        raise ValueError("malformed bbl '%s'" % bbl)

def make_path(bbl):
    t = split_bbl(bbl)
    return "%.1d/%.5d/%.4d/" % t

def make_bbl_dir(topdir,bbl):
    """(Softly) creates the parent dir structure for a given BBL."""
    boro,block,lot = split_bbl(bbl)
    p1 = "%s/%.1d" % (topdir,boro)
    p2 = "%s/%.5d" % (p1,block)
    p3 = "%s/%.4d" % (p2,lot)
    for p in (p1,p2,p3):
        mkdir_soft(p)
    return p

# deprecated
def _make_bbl_dir(topdir,bbl):
    """(Softly) creates the parent dir structure for a given BBL."""
    boro,block,lot = split_bbl(bbl)
    p1 = "%s/%.1d" % (topdir,boro)
    p2 = "%s/%.5d" % (p1,block)
    for p in (p1,p2):
        mkdir_soft(p)
    return p

def mkdir_soft(dirname):
    """Creates a directory (unless it already exist).  Because it's entirely
    possible that another process can be attempting to create a directory at the
    same location at about the same time -- such that another process comes in to
    create the directory, in between the existence check and the mkdir statement --
    the execption will be caught (so from the perspective of the calling context, 
    it's as if the collision never happened)."""
    if not os.path.exists(dirname):
        try:
            os.mkdir(dirname)
        except FileExistsError as e:
             pass
    return dirname

def init_stash(args):
    if not os.path.exists(args.stash):
        raise ValueError("Can't find stash dir '%s'" % args.stash)
    if not is_valid_date_spec(args.spec):
        raise ValueError("Invalid date spec '%s'" % args.spec)
    d = OrderedDict()
    d['meta'] = "%s/meta" % args.stash
    d['output'] = "%s/output" % args.stash
    d['spec'] = "%s/%s" % (args.stash,args.spec)
    d['data'] = "%s/data" % d['spec']
    d['pull'] = "%s/pull" % d['spec']
    for path in d.values():
        mkdir_soft(path)
    return d

def save_list_like(dirpath,d,name):
    """
    Saves 'list-like' structures referenced as values in a dict.
    By that we mean objects that are either lists or which can be cast as lists,
    for example sets, iterators, etc.  If presented as a list, items are dumped
    in order; otherwise they're sorted first.
    """
    print("%s:" % name)
    for k in sorted(d.keys()):
        values = d[k] if isinstance(d[k],list) else sorted(d[k])
        outfile = "%s/%s-%s.txt" % (dirpath,name,k)
        print("%s '%s': %d to %s .." % (name,k,len(values),outfile))
        ioany.save_lines(outfile,d[k])



# XXX deprecated
_expand = {
    'Q1':'01-01',
    'Q2':'04-03',
    'Q3':'07-01',
    'Q4':'10-01',
}
def expand_date_spec(spec):
    """Converts a date spec to activity date, e.g. '2017Q2' => '2017-07-01'"""
    if is_valid_date_spec(spec):
        year = int(spec[:4])
        quarter = spec[4:]
        mmdd = _expand.get(quarter)
        if mmdd is not None:
            return "%.4d-%s" % (year,mmdd)
    raise ValueError("invalid date spec '%s'" % spec)



