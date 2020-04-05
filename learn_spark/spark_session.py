from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster('local').setAppName('second_app')
sc = SparkContext(conf=conf)
iphones = [('XS', 2018, 5.65, 2.79, 6.24),
           ('XR', 2018, 5.94, 2.98, 6.84),
           ('X10', 2017, 5.65, 2.79, 6.13),
           ('8Plus', 2017, 6.23, 3.07, 7.12)]

iphones_RDD = sc.parallelize(iphones)

names = ['Model', 'Year', 'Height', 'Width', 'Weight']
spark = SparkSession(sparkContext=sc)
df = spark.createDataFrame(iphones_RDD, schema=names)

df.createOrReplaceTempView("iphone_table")
query = """SELECT Year FROM iphone_table"""

new_df = spark.sql(query)
new_df.show()

pandas_df = df.toPandas()
print(type(pandas_df))