from collections import OrderedDict

def condense(stream):
    bblprev,r = None,None
    for t in stream:
        bblcurr,section,attribute,value = t
        if bblprev is None:
            bblprev = bblcurr
            r = OrderedDict()
        elif bblcurr != bblprev:
            yield bblprev,r
            bblprev = bblcurr
            r = OrderedDict()
        if section not in r:
            r[section] = []
        r[section] += [(attribute,value)]
    if bblprev is not None:
        yield bblprev,r

def dictify(r):
    return OrderedDict((k,OrderedDict(v)) for k,v in r.items())

def collect(stream):
    for bbl,r in condense(stream):
        yield bbl,dictify(r)

