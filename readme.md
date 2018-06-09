用于理解spark的迭代计算原理，实现了MapPartitionRDD,ParallelCollectionRDD,以及map foreach方法:
```python
parallelize(range(10)).filter(lambda x:x%2==0).foreach(print)
# 0 2 4 6 8
```
具体的原理请参照博客[迭代计算](https://lishion.github.io/2018/06/06/Spark-%E6%BA%90%E7%A0%81%E9%98%85%E8%AF%BB%E8%AE%A1%E5%88%92-%E7%AC%AC%E4%B8%80%E9%83%A8%E5%88%86-%E8%BF%AD%E4%BB%A3%E8%AE%A1%E7%AE%97/)