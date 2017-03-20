import time
from functools import wraps


def timedsingle(func):
    @wraps(func)
    def called(*args,**kwargs):
        t0 = time.time()
        x = func(*args,**kwargs)
        delta = time.time() - t0
        return x,delta
    return called

def timedmulti(func):
    @wraps(func)
    def called(*args,**kwargs):
        t0 = time.time()
        t = func(*args,**kwargs)
        delta = time.time() - t0
        return tuple(list(t) + [delta])
    return called

