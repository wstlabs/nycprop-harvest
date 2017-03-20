import sys

from crawl.agent import Agent



STYPE = 'SOA'
pubdate = '20170224'

bbl = int(sys.argv[1])
agent = Agent()

print("search..")
r = agent.search(bbl)
print(r.status_code)
print(r.headers)
print("grab..")
r = agent.grab(bbl,pubdate,STYPE)
print(r.status_code)
print(r.headers)

outfile = "%s-%d.pdf" % (STYPE,bbl)
print("write to '%s' .." % outfile)
with open(outfile, 'wb') as f:
   f.write(r.content)
print("done!")

