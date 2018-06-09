from concurrent.futures import ThreadPoolExecutor,wait

class ResultTask(object):
    def __init__(self,rdd,part,func):
       self._rdd = rdd
       self._part = part
       self._func = func
       
    def runTask(self):
        myPart = self._rdd.getPartitions()[self._part]
        result = self._func(self._rdd.itertor(myPart))
        return result

def submitJob(rdd,partsToCalc,func):
    tasks = [ResultTask(rdd,part,func) for part in partsToCalc]
    numParts = len(tasks)
    with ThreadPoolExecutor(max_workers=numParts) as pool:
        executors = [pool.submit(task.runTask) for task in tasks]
        return [executor.result() for executor in executors]

 