from Partition import ParallelCollectionPartition
from abc import ABCMeta,abstractclassmethod
from Schedule import submitJob
from functools import reduce

class RDD(object):
    def getPartitions(self):
        pass

    def compute(self,split):
        pass
    
    def itertor(self,split):
        return iter(self.compute(split))
    
    def count(self):
        result = submitJob(self,range(0,len(self.getPartitions())),lambda x:len(list(x)))
        return reduce(lambda x,y:x+y,result,0)

    def foreach(self,func):
        def _foreach(x,func):
            for item in x:
                func(item)
        submitJob(self,range(0,len(self.getPartitions())),lambda x:_foreach(x,func))

    def map(self,mapFunction):
        return MapPartitionsRDD(self,lambda part:map(mapFunction,part))
    
    def filter(self,filterFunction):
        return MapPartitionsRDD(self,lambda part:filter(filterFunction,part)  )

class MapPartitionsRDD(RDD):
    def __init__(self,parent,function):
        self._parent = parent
        self._function = function
    
    def compute(self,split):
        return self._function(self._parent.itertor(split))
    
    def getPartitions(self):
        return self._parent.getPartitions()

class ParallelCollectionRDD(RDD):

    @staticmethod
    def slice(data,numSlices):
        """
        data: 切分的数据
        numSlices: 最终需要切分的份数
        这里采用的较为简单的切分算法如输入　０,1,2,3,4　切分为３份
        输出为: [[0],[1],[2,3,4]]
        """
        numData = len(data)
        assert(numSlices > 0 and numSlices <= numData)
        equable = len(data) % numSlices == 0
        eachNumPart = numData // numSlices 
        parts = []
        for i in range(numSlices):
            start = i * eachNumPart 
            end = numData - 1 if not equable and i == numSlices - 1  else  start + eachNumPart -1
            parts.append(data[start:end+1])
        return parts

    def __init__(self,data,numSlices):
        self._data = data
        self._numSlices = numSlices

    def getPartitions(self):
        slices = ParallelCollectionRDD.slice(self._data, self._numSlices)
        return [ ParallelCollectionPartition(x,slices[x]) for x in range(len(slices))]

    def itertor(self,split):
        return split.itertor()

def parallelize(data,numSlices = 4):
    return ParallelCollectionRDD(data,numSlices)


   