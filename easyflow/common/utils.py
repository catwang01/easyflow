from typing import Callable, List, Optional
from multiprocessing import Pool

def multiprocessingRun(func: Callable, argsList: List, pool: Optional[Pool]=None, wait: bool=False):
    if pool is None:
        pool = Pool()
    results = []
    for args in argsList:
        if not isinstance(args, list):
            args = (args, )
        # 用error_callback 来处理使用子进程的错误信息，否则子进程如果报错也不会出现错误提示
        result = pool.apply_async(func, args=args, error_callback=print)
        results.append(result)
    if wait:
        pool.close()
        pool.join()
        results = [result.get() for result in results]
    return results
