import hashlib
import logging
import sys
from datetime import datetime
from functools import wraps
from time import sleep

from pymongo.errors import AutoReconnect, OperationFailure, DuplicateKeyError, ServerSelectionTimeoutError

from .hooks import log_exception as _log_exception

logger = logging.getLogger(__name__)

_MAX_RETRIES = 15


def _get_host(store):
    ret = {}
    if store:
        try:
            if isinstance(store, (list, tuple)):
                store = store[0]
            ret['l'] = store._arctic_lib.get_name()
            ret['mnodes'] = ["{}:{}".format(h, p) for h, p in store._collection.database.client.nodes]
            ret['mhost'] = "{}".format(store._arctic_lib.arctic.mongo_host)
        except Exception:
            # Sometimes get_name(), for example, fails if we're not connected to MongoDB.
            pass
    return ret


_in_retry = False
_retry_count = 0


def mongo_retry(f):
    """
    Catch-all decorator that handles AutoReconnect and OperationFailure
    errors from PyMongo
    """
    log_all_exceptions = 'arctic' in f.__module__ if f.__module__ else False

    @wraps(f)
    def f_retry(*args, **kwargs):
        global _retry_count, _in_retry
        top_level = not _in_retry
        _in_retry = True
        try:
            while True:
                try:
                    return f(*args, **kwargs)
                except (DuplicateKeyError, ServerSelectionTimeoutError) as e:
                    # Re-raise errors that won't go away.
                    _handle_error(f, e, _retry_count, **_get_host(args))
                    raise
                except (OperationFailure, AutoReconnect) as e:
                    _retry_count += 1
                    _handle_error(f, e, _retry_count, **_get_host(args))
                except Exception as e:
                    if log_all_exceptions:
                        _log_exception(f.__name__, e, _retry_count, **_get_host(args))
                    raise
        finally:
            if top_level:
                _in_retry = False
                _retry_count = 0
    return f_retry


def _handle_error(f, e, retry_count, **kwargs):
    if retry_count > _MAX_RETRIES:
        logger.error('Too many retries %s [%s], raising' % (f.__name__, e))
        e.traceback = sys.exc_info()[2]
        raise
    log_fn = logger.warning if retry_count > 2 else logger.debug
    log_fn('%s %s [%s], retrying %i' % (type(e), f.__name__, e, retry_count))
    # Log operation failure errors
    _log_exception(f.__name__, e, retry_count, **kwargs)
#    if 'unauthorized' in str(e):
#        raise
    sleep(0.01 * min((3 ** retry_count), 50))  # backoff...


def mongo_memoize(mongo_conn, ttl=60*60, prefix='memoize'):
    """
    Decorators that uses mongo to cache the return values of a function for the provided ttl.

    :param mongo_conn: Mongo connection object
    :param ttl: Total time to cache the libraries.
    :param prefix: prefix for the collection that stores cache data in mongo.
    :return: Decorated function that will execute if the data is not present
    in the collection or the cached value is returned. Note: The return value
    of the function must be trivially serializable by mongo.
    """
    def decorator(func):
        col_name = '{}_{}_{}'.format(prefix, func.__name__, hashlib.md5(func.__module__).hexdigest())
        mongo_db = mongo_conn.memoize_db
        if col_name not in mongo_db.collection_names():
            new_collection = mongo_db.create_collection(col_name)
            new_collection.create_index("date", expireAfterSeconds=ttl)

        cache_col = mongo_db[col_name]

        @wraps(func)
        def memoized_f(*args, **kwargs):
            coll_data = cache_col.find_one()
            if coll_data:
                return coll_data['cached_data']
            ret = func(*args, **kwargs)
            cache_col.remove({})
            # TODO: Check if ret is serializable to BSON by mongo.
            cache_col.insert({"date": datetime.utcnow(), "cached_data": ret})
            return ret

        return memoized_f

    return decorator
