import re
from functools import reduce
from collections import OrderedDict
from crawl.pdfutil import textify
from crawl.logging import log

pat = {}
pat['integer'] = re.compile('^\d+$')
pat['abatement'] = re.compile('^.*abatement',re.IGNORECASE)
pat['statement-billing-summary'] = re.compile('^.*Statement Billing Summary',re.IGNORECASE)
def matches_pattern(page,name):
    regex = pat[name]
    for line in page:
        if re.match(regex,line):
            return True
    return False



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

def yield_index(page,label):
    for i,line in enumerate(page):
        if line.startswith(label):
            yield i

def yield_after(page,label,offset=1):
    for i,line in enumerate(page):
        if line.startswith(label):
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

def scrub_amount(s):
    return None if s is None else s.replace(",","").replace("$","")

def extract_amount_after(page,label):
    found = False
    for i,line in enumerate(page):
        if line.startswith(label):
            found = True
        elif found and line.startswith("$"):
            return line

def extract_total_amount_due(bigpage):
    balance = extract_after(bigpage,'Total amount due by',-1,2)
    if balance is not None and balance.startswith("$"):
        return balance
    return extract_amount_after(bigpage,'Total amount due by')

def extract_generic_amount_due(page):
    return extract_after(page,'Amount Due',0)

def parse_general(pages,bigpage):
    x = OrderedDict()
    x['htype'] = 1 if matches_pattern(pages[0],'statement-billing-summary') else 2
    x['tax-class'] = extract_after(bigpage,'Tax class',0)
    x['owner-name'] = extract_between(pages[0],'Owner name:','Property address:')
    x['mailing-address'] = extract_mailing_address(pages[0])
    market_value = extract_after(bigpage,'Estimated market value',0)
    x['estimated-market-value'] = scrub_amount(market_value)
    if x['htype'] == 1:
        balance = extract_total_amount_due(bigpage)
    else:
        balance = extract_generic_amount_due(pages[0])
    x['total-amount-due'] = scrub_amount(balance)
    return x

def extract_unitcount(page):
    """Extracts what appears to be the stabilized unit count from a given page.
    Our current hypothesis is that whenever string appears directly after the
    label 'Housing-Rent Stabilization', -and- this string looks like an integer,
    then that's the unit count (and this number will be the same regardless of
    how many times such a match occurs throughout the page).
    If more than one match is detected, we simply emit the first matching value
    and make a note of this weirdness in the logs."""
    rawvals = list(yield_after(page,'Housing-Rent Stabilization'))
    log.info("RAW %s" % rawvals)
    intlike = (int(_) for _ in rawvals if re.match(pat['integer'],_))
    counts  = sorted(set(intlike))
    if len(counts) < 1:
        if rawvals:
            log.info("WEIRD rawvals = %s but none are integer" % rawvals)
        return None
    if len(counts) > 1:
        log.info("WEIRD too many unitcount values %s" % counts)
    return counts[0]

def parse_stabilization(pages,bigpage):
    x = OrderedDict()
    x['unitcount'] = extract_unitcount(bigpage)
    return x

def parse_abatements(pages,bigpage):
    x = OrderedDict()
    # x['exist'] = 'yes' if list(yield_index(bigpage,'Tax before abatements')) else 'no'
    x['special-interest'] = 'yes' if list(yield_index(bigpage,'Spec Init Pgm')) else 'no'
    return x

def parse(f):
    pages = list(textify(f))
    bigpage = reduce(lambda x, y: x+y,pages)
    d = OrderedDict()
    d['general']       = parse_general(pages,bigpage)
    d['stabilization'] = parse_stabilization(pages,bigpage)
    d['abatements']    = parse_abatements(pages,bigpage)
    return d



