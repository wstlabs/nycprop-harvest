import re
from collections import OrderedDict
import dateutil.parser




def parse_float(s):
    return float(s.replace(",",""))

def parse_date(s):
    return dateutil.parser.parse(s).date()

amountpat = re.compile('^\$([\d,\.\-]+)$')
def parse_amount(s):
    m = re.match(amountpat,s)
    return parse_float(m.group(1)) if m else None

breakpat = re.compile('^1400\.01.*$')
def extract_first_page(text):
    block = []
    for line in text:
        if re.match(breakpat,line):
            return block
        else:
            block += [line]
    return None

wordpat = re.compile('^\s*(\S+)')
def first_word(s):
    m = re.match(wordpat,s)
    return m.group(1) if m else None


def indexof(text,s):
    for i,line in enumerate(text):
        if line.strip() == s.strip():
            return i
    return -1


def _indexof_starting_forward(text,s):
    for i,line in enumerate(text):
        if line.startswith(s):
            return i
    return -1

def _indexof_starting_reverse(text,s):
    n = len(text)
    for i in range(n-1,-1,-1):
        if text[i].startswith(s):
            return i
    return -1

def indexof_startswith(text,s,reverse=False):
    if reverse:
        return _indexof_starting_reverse(text,s)
    else:
        return _indexof_starting_forward(text,s)


def _indexof_matching_forward(text,pat):
    for i,line in enumerate(text):
        if re.match(pat,line):
            return i
    return -1

def _indexof_matching_reverse(text,pat):
    n = len(text)
    for i in range(n-1,-1,-1):
        if re.match(pat,text[i]):
            return i
    return -1

def indexof_matching(text,pat,reverse=False):
    if reverse:
        return _indexof_matching_reverse(text,pat)
    else:
        return _indexof_matching_forward(text,pat)



dollarpat = re.compile('^\s*(\$[\d,\.\-]+).*$')
def extract_dollar_sequence(text):
    for line in text:
        m = re.match(dollarpat,line)
        if m:
            yield m.group(1)

def strip_block(text):
    nonempty = re.compile('^.*\S')
    start = indexof_matching(text,nonempty)
    end = indexof_matching(text,nonempty,reverse=True)
    if start >= 0:
        return text[start:end+1]
    else:
        return text

def reduce_block(text):
    for line in text:
        if line.strip() != '':
            yield line


niftyset = set([
    'Previous','Amount','Interest','Unpaid','Current','If','Total','Charges','You,',
    'Payment','Overpayments/credits'])
def skrunch_block(text):
    current = None
    for line in text:
        line = line.strip()
        first = first_word(line)
        if first in niftyset:
            if current is not None:
                yield current
            current = line
        else:
            if current is not None:
                current = current + " :: " + line
            else:
                current = line
    if current is not None:
        yield current


def split_body(text):
    block1,block2 = [],[]
    for line in text:
        line = line.strip()
        if line == '':
            continue
        if line.startswith('$'):
            block2 += [line]
        else:
            block1 += [line]
    return block1,block2


def dump_block(block,label):
    for i,line in enumerate(block):
        print(":: %s-%d [%s]" % (label,i,line))

def extract_body_class1(text):
    """
    Extract what appears to be the "body" of the first page, that is,
    everything after the legend - and ignoring rare occurenes of misplaced
    header lines (which sometimes get emitted below the legend, as an artefact
    of our primitive PDF mining process).
    """
    legend = indexof(text,'Statement Billing Summary')
    for line in text[legend+1:]:
        if line.startswith("Owner name"):
            continue
        if line.startswith("Property address"):
            continue
        if line.startswith("Borough,"):
            continue
        yield line

def parse_class1(text):
    legend = indexof(text,'Statement Billing Summary')
    body = extract_body_class1(text)
    block1,block2 = split_body(body)
    block1 = list(skrunch_block(block1))
    status = len(block1) == len(block2)
    if not status:
        dump_block(block1,"block1")
        dump_block(block2,"block2")
    align = "%d-%d" % (len(block1),len(block2))
    # Identify the value opposite from the "Amount Due" heading 
    j = indexof_startswith(block1,"Total amount due")
    amount = parse_amount(block2[j]) if j >= 0 else None
    return status,align,amount

simpleset = set(['Outstanding Charges','New Charges','Amount Due'])
def extract_block1_class2(text):
    for line in text:
        if line in simpleset:
            yield line

def parse_class2(text):
    """
    Outstanding Charges
    New Charges
    Amount Due
    Please pay by July 1, 2016
    Visit us at nyc.gov/finance or call 311 for more information.]
    Did you know you can pay your property taxes using your smartphone?
    Visit any of our Business Centers to pay using mobile wallet!
    """
    begin = min(indexof(text,'Outstanding Charges'),indexof_startswith(text,'$'))
    body = text[begin:]
    block1,block2 = split_body(body)
    block1 = list(extract_block1_class2(block1))
    status = len(block1) == len(block2)
    if not status:
        dump_block(block1,"block1")
        dump_block(block2,"block2")
    align = "%d-%d" % (len(block1),len(block2))
    # Identify the value opposite from the "Amount Due" heading 
    j = indexof(block1,"Amount Due")
    amount = parse_amount(block2[j]) if j >= 0 else None
    return status,align,amount

def determine_class(text):
    if indexof(text,"Statement Billing Summary") >= 0:
        return 1
    elif indexof(text,"Outstanding Charges") >= 0:
        return 2
    else:
        return 3

_pagehandler = { 1:parse_class1, 2:parse_class2 }
def parse_page(text,klass):
    handler = _pagehandler.get(klass)
    x = OrderedDict()
    if handler:
        status,align,amount = handler(text)
        # x["status-%d" % klass] = status
        # x["align-%d" % klass] = align
        x['align-status'] = status
        x['align-depth'] = align
        x['amount'] = amount
    return x


def parse_page1(text):
    dollars = list(extract_dollar_sequence(text))
    j = determine_class(text)
    x = OrderedDict()
    x['class'] = j
    y = parse_page(text,j)
    x.update(y)
    return x

def parse(text):
    page1 = extract_first_page(text)
    if page1:
        x = parse_page1(page1)
    else:
        x = OrderedDict()
        x['error'] = 'no-page-break'
    d = OrderedDict()
    d['statement'] = x
    return d

def process():
    pass


look = OrderedDict()

"""
Matches lines that take one of two forms:

   Total amount due by October 1, 2012                                                     $12,583.96
   Total amount due by July 1, 2016. To avoid interest pay on or before July 15th.         $64,963.11
"""
look['activity-through'] = {
    'regex': '^\s*Activity through\s+(\S+\s+\d+,\s+\d+)\s*$',
    'names': ('activedate',),
    'parse': (parse_date,),
}

_vanilla = [
    ('outstanding-charges','^\s*Outstanding Charges.*\$(\S+)\s*$'),
    ('new-charges',        '^\s*New Charges.*\$(\S+)\s*$'),
    ('amount-due',         '^\s*Amount Due.*\$(\S+)\s*$'),
    ('previous-charges',   '^\s*Previous charges.*\$(\S+)\s*$'),
    ('unpaid-charges',     '^\s*Unpaid charges.*\$(\S+)\s*$'),
    ('everything-you-owe', '^.*want to pay everything.*\$(\S+)\s*$'),
]
for k,v in _vanilla:
    look['header-'+k] = {
        'regex': v,
        'names': ('amount',),
        'parse': (parse_float,),
    }



look['total-amount-due'] = {
    'regex': '^\s*Total amount due by\s+(\S+\s+\d+,\s+\d+)\.?\s+.*\s+\$(\S+)\s*$',
    'names': ('duedate','balance',),
    'parse': (parse_date,parse_float)
}

"""
Matches lines which generally look something like this:

   Tax class 2 - Residential, More Than 10 Units          Tax rate

But which might be missing the "Tax rate" part at the end, or have more stuff after it.
All we care about, though is the \S+ string between "Tax class" and the "-".
"""
look['tax-class'] = {
    'regex': '^\s*Tax\s+class\s+(\S+)\s+\-',
    'names': ('taxclass',),
    'parse': (None,),
}







def extract_tuples(text,name,cast=True):
    """A generator which extracts tuples of raw strings matching the given named pattern."""
    for line in text:
        m = re.match(look[name]['regex'],line)
        if m:
            values = m.groups()
            if cast:
                parsers = look[name]['parse']
                yield tuple(apply(f,v) for f,v in zip(parsers,values))
            else:
                yield values

def extract_dicts(text,name,cast=True):
    names = look[name]['names']
    for values in extract_tuples(text,name,cast=True):
        pairs = zip(names,values)
        yield OrderedDict(pairs)

def apply(f,s):
    if f is None:
        return s
    try:
        return f(s)
    except Exception as e:
        print("can't apply parser %s on '%s', reason: %s" % (f,s,e))
        print(e)
        raise RuntimeError("no good")


def _analyze(text):
    pairs = ((name,list(extract_dicts(text,name))) for name in look.keys())
    return OrderedDict(pairs)


def crank(text,cast=True):
    missed = []
    for name in look.keys():
        tt = list(extract_tuples(text,name,cast))
        if len(tt) == 0:
            missed += [name]
        for t in tt:
            print(name,*t)
    return missed


