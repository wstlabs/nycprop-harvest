import time
from functools import wraps

def backoff(retry=3,wait=5,log=None):
    # @wraps
    def wrapped(f):
        def safely(*args,**kwargs):
            for count in range(retry):
                try:
                    if log:
                        log.debug("count = %d" % count)
                    return f(*args,**kwargs)
                except Exception as e:
                    if log:
                        log.info("count = %d, reason = %s" % (count,e))
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



