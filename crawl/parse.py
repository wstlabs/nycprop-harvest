import re
from collections import OrderedDict
from crawl.pdfutil import textify
from crawl.logging import log


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

def extract_after(page,label,offset=1):
    for i,line in enumerate(page):
        if line == label:
            if i+offset < len(page):
                return page[i+offset]
            else:
                log.debug("weirdness: overflow for label='%s', offset=%d" % (label,offset))
    return None

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

def extract_first(text,label,offset=1):
    for j,page in enumerate(text):
        value = extract_after(page,label,offset)
        if value is not None:
            return value
    return None

def extract_mailing_address(page):
    for endlabel in ('Owner name:','1400.'):
        address = extract_between(page,'Mailing address:',endlabel)
        if address is not None:
            return address
    return extract_first(page,'Mailing address:')


def parse(f):
    text = list(textify(f))
    d = OrderedDict()
    x = OrderedDict()
    x['tax-class'] = extract_first(text,'Tax class')
    x['owner-name'] = extract_between(text[0],'Owner name:','Property address:')
    x['mailing-address'] = extract_mailing_address(text[0])
    d['general'] = x
    x = OrderedDict()
    x['count'] = extract_first(text,'Housing-Rent Stabilization')
    d['stabilization'] = x
    return d
