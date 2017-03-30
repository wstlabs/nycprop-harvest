import re
from functools import reduce
from collections import OrderedDict
from crawl.pdfutil import textify
from crawl.logging import log



def extract_between(page,startlabel,endlabel,offset=1):
    block = None
    for i,line in enumerate(page):
        if line.startswith(endlabel):
            return block
        if line.startswith(startlabel):
            block = []
        elif block is not None:
            block.append(line)
    return None

def yield_after(page,label,offset=1):
    for i,line in enumerate(page):
        if line == label:
            if i+offset < len(page):
                yield page[i+offset]
            else:
                log.debug("weirdness: overflow for label='%s', offset=%d" % (label,offset))


def _extract_after_pos(page,label,index=0,offset=1):
    for j,value in enumerate(yield_after(page,label,offset)):
        if j == index:
            return value
    return None

def _extract_after_neg(page,label,index,offset=1):
    """The extract_after function, limited to the negative index case.
    This one is inherently more expensive computationaly because we have to
    process (and store) all the intermediate matches before we know where
    to start counting backwards from."""
    if index >= 0:
        raise ValueError("invalid usage - expected negative index")
    values = list(yield_after(page,label,offset))
    j = len(values) + index
    return values[j] if j>= 0 else None

def extract_after(page,label,index=0,offset=1):
    f = _extract_after_pos if index >= 0 else _extract_after_neg
    return f(page,label,index,offset)

def extract_mailing_address(page):
    for endlabel in ('Owner name:','1400.'):
        address = extract_between(page,'Mailing address:',endlabel)
        if address is not None:
            return address
    return extract_first(page,'Mailing address:')


def parse(f):
    text = list(textify(f))
    bigpage = reduce(lambda x, y: x+y,text)
    d = OrderedDict()
    x = OrderedDict()
    x['tax-class'] = extract_after(bigpage,'Tax class',0)
    x['owner-name'] = extract_between(text[0],'Owner name:','Property address:')
    x['mailing-address'] = extract_mailing_address(text[0])
    d['general'] = x
    x = OrderedDict()
    x['count'] = extract_after(bigpage,'Housing-Rent Stabilization',-1)
    d['stabilization'] = x
    return d




#
# deprecated stuff
#



pat = {}
pat['abatement'] = re.compile('^.*abatement',re.IGNORECASE)
def _has_abatement(block):
    for line in block:
        if re.match(pat['abatement'],line):
            return True
    return False

def _extract_abatement(block):
    for line in block:
        if re.match(pat['abatement'],line):
            return line
    return None

