from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster('local').setAppName('first_app')
sc = SparkContext(conf=conf)
regularRDD = sc.parallelize([('Mary', 23), ('Sam', 32), ('John', 28), ('Walliam', 25), ('Sam', 26)])
pairRDD = regularRDD.reduceByKey(lambda x, y: x + y)
print(pairRDD.collect())
pairRDD_rev = pairRDD.map(lambda x:(x[1],x[0]))
print(pairRDD_rev.sortByKey(ascending=False).collect())

airports = [('UK', 'LHR'), ('UK','LGW'), ('CN', 'BJI'), ('CN', 'SHI'), ('US', 'NYI')]
airportRDD = sc.parallelize(airports)
rdd_group = airportRDD.groupByKey().collect()
for count, air in rdd_group:
    print(count, list(air))