import sys
from crawl.parse import parse
import ioany

infile = sys.argv[1]
lines = ioany.slurp_lines(infile)
print("that be %d lines." % len(lines))
t = parse(lines,'total-amount-due')
print(t)
t = parse(lines,'tax-class')
print(t)
