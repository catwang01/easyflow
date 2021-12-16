import time
import sys
from typing import Callable, List, Optional
from platform import system
from multiprocessing.pool import Pool
import multiprocessing

def multiprocessingRun(func: Callable, argsList: List, pool: Optional[Pool]=None, wait: bool=False):
    if pool is None:
        pool = multiprocessing.Pool()
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

def platform() -> str:
    return system()

class Timer:

    def __init__(self, descStart="Timer start!", descEnd="Timer end!", verbose=True, stdout=sys.stdout):
        self.descStart = descStart
        self.descEnd = descEnd
        self.stdout = stdout
        self.verbose = verbose
        self.clock = self.Clock()

    def __enter__(self):
        if self.verbose:
            self.stdout.write(f"{self.descStart}\n")
        self.clock.setStartTime(time.time())
        return self.clock

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clock.setEndTime(time.time())
        if self.verbose:
            self.stdout.write(f"{self.descEnd}\n")

    class Clock:
        def __init__(self) -> None:
            self.startTime = 0
            self.endTime = 0
        
        def setStartTime(self, startTime):
            self.startTime = startTime

        def setEndTime(self, endTime):
            self.endTime = endTime

        