import sys

from crawl.agent import Agent



bbl = 1014880013
agent = Agent()
# r = agent.connect()
r = agent.search(bbl)
print(r.status_code)
print(r.headers)
print(r.text)

