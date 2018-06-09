from RDD import parallelize
parallelize(range(10)).filter(lambda x:x%2==0).foreach(print)