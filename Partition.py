class ParallelCollectionPartition():
    def __init__(self,index,data):
        self._index = index
        self._data = data
    def itertor(self):
        return iter(self._data)