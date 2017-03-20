import os
import sys
import logging

appbase = os.path.basename(sys.argv[0])
appname,ext = os.path.splitext(appbase)
logdir = 'log'
if not os.path.exists(logdir):
    os.mkdir(logdir)

logging.basicConfig(
    filename = "%s/%s-%d.log" % (logdir,appname,os.getpid()),
    format   = "%(levelname)s %(funcName)s %(message)s",
    level    = logging.INFO
)
log = logging.getLogger('app')

# Downgrade logging for the 'requesst' package.
reqlog = logging.getLogger("requests")
if reqlog:
    reqlog.setLevel(logging.WARNING)


# Quick-and-dirty hack to downgrade logging in at least those submodules
# of the pdfminer.* namespace that were cluttering our INFO stream.
# (There may be other modules with logging, activated under different
# use cases, but these were the ones showing up for our use case).
disable = (
   'converter',
   'pdfdocument',
   'pdfinterp',
   'pdfpage',
)
for k in disable:
    badlog = logging.getLogger("pdfminer.%s" % k)
    if badlog:
        badlog.setLevel(logging.WARNING)

# print("name = [%s]" % __name__)
# sys.exit(1)
