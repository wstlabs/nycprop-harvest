import time
from functools import wraps

def backoff(retry=3,wait=5,log=None):
    if log:
        log.info("retry=%s, wait=%s" % (retry,wait))
    # @wraps
    def wrapped(f):
        if log:
            log.info("wrap!")
        def safely(*args,**kwargs):
            if log:
                log.info("safely...")
            for count in range(retry):
                try:
                    if log:
                        log.info("count = %d" % count)
                    return f(*args,**kwargs)
                except Exception as e:
                    if log:
                        log.error("count = %d, reason = %s" % (count,e))
                        log.exception(e)
                    if count+1 < retry:
                        if log:
                            log.info(":: SLEEP %d .." % wait)
                        time.sleep(wait)
            message = "retry limit exceeded, count = %d" % count
            if log:
                log.error(message)
            raise RuntimeError(message)
        return safely
    return wrapped



