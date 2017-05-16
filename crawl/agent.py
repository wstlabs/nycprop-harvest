import time
import requests
from collections import OrderedDict
from crawl.util import split_bbl
from crawl.backoff import backoff
import crawl.constants
from crawl.logging import log



# siteurl = 'http://nycprop.nyc.gov/nycproperty/nynav/jsp/selectbbl.jsp'
# posturl = 'http://webapps.nyc.gov:8084/CICS/fin1/find001i'
# searchurl = 'http://nycprop.nyc.gov/nycproperty/nynav/jsp/stmtassesslst.jsp'
#     url = "http://nycprop.nyc.gov/nycproperty/StatementSearch?bbl=%s&stmtDate=%s&stmtType=SOA" % (bbl,date)
# url = 'http://nycprop.nyc.gov/nycproperty/StatementSearch?bbl=1014880013&stmtDate=20170224&stmtType=SOA'
# url = "http://nycprop.nyc.gov/nycproperty/StatementSearch?bbl=%s&stmtDate=%s&stmtType=SOA" % (bbl,date)

siteurl = "http://nycprop.nyc.gov"
searchurl = siteurl + "/nycproperty/nynav/jsp/stmtassesslst.jsp"

def doc_url(bbl,date,stype):
    path = "/nycproperty/StatementSearch?bbl=%s&stmtDate=%s&stmtType=%s" % (bbl,date,stype)
    return siteurl + path


def makequery(bbl):
    boro,block,lot = split_bbl(bbl)
    block = "%.5d" % block
    lot = "%.4d" % lot
    pairs = [
        ('FFUNC','C'),
        ('q49_boro',boro),
        ('q49_block_id',block),
        ('q49_lot',lot),
        ('q49_prp_ad_street_no',''),
        ('q49_prp_nm_street',''),
        ('q49_prp_id_apt_num',''),
        ('q49_prp_ad_city',''),
        ('q49_prp_cd_state',''),
        ('q49_prp_cd_addr_zip',''),
        ('bblAcctKeyIn1',boro),
        ('bblAcctKeyIn2',block),
        ('bblAcctKeyIn3',lot),
        ('bblAcctKeyIn4',''),
        ('ownerName',''),
        ('ownerName1',''),
        ('ownerName2',''),
        ('ownerName3',''),
        ('ownerName4',''),
        ('ownercount',1),
        ('returnMsg','blahblah'),
    ]
    return OrderedDict(pairs)

WAIT = 60
MAXFAIL = 3

class Agent(object):

    def __init__(self):
        self.s = requests.session()
        self.s.headers.update({'User-Agent': 'Mozilla/5.0'})
        self._fails = 0

    @backoff(retry=3,wait=120,log=log)
    def get(self,url,**kwargs):
        log.debug("url = %s" % url)
        r = self.s.get(url,**kwargs)
        log.info("GET status = %s" % r.status_code)
        log.debug("GET r.headers = %s" % r.headers)
        return r

    @backoff(retry=3,wait=120,log=log)
    def post(self,url,**kwargs):
        log.debug("url = %s" % url)
        r = self.s.post(url,**kwargs)
        log.info("POST r.status = %s" % r.status_code)
        log.debug("POST r.headers = %s" % r.headers)
        return r

    def search(self,bbl):
        data = makequery(bbl)
        return self.post(searchurl,data=data)

    def grab(self,bbl,date,stype):
        assert_valid_stype(stype)
        url = doc_url(bbl,date,stype)
        return self.get(url)


def assert_valid_stype(stype):
    if stype not in crawl.constants.valid_statement_types:
        raise ValueError("invalid statement type '%s'" % stype)

