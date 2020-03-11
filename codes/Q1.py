import matplotlib
matplotlib.use('Agg')


from pyspark import SparkConf, SparkContext
import pandas as pd
import os
import csv
from datetime import datetime
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt


#Function to return the time witch started the trip
def getDuration(arg):

    line = arg.split(",")
    hour =  int(line[1].split(' ')[1].split(':')[0])
    datetime_object1 = datetime.strptime(line[1].split(" ")[-1], '%H:%M:%S')  #arg1 of strpite is start Time of trip
    datetime_object2 = datetime.strptime(line[2].split(" ")[-1], '%H:%M:%S')  #arg1 of strtype is finish Time of trip
    timeMin = ((datetime_object2 - datetime_object1).total_seconds() / 60)
    return (hour,timeMin)
    




#-----------------INITIALIZATIONS--------------------------------
conf = SparkConf().setAppName("MapReduce approach")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
taxidata = sc.textFile("hdfs://master:9000/user/user/data/yellow_tripdata_1m.csv")
start_time1 = time.time()
timeres = []

#PART 1
#Q1 : Average Trip Duration by hour of Starting Time of Trip 
#Start with Map
taxidata = taxidata.map(lambda line: (getDuration(line))) \
    .aggregateByKey((0,0), lambda a,b: (a[0] + b, a[1] +1),lambda a,b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: v[0]/v[1]) \
    .sortBy(lambda a: a[0]).collect()

print "         Q1            "
print "-----------------------"
print "Time    |   Avg Time   "
for i in taxidata:
    print taxidata[0] , " | ", taxidata[1]

print("--- %s seconds for Q1 using RDD ---" % (time.time() - start_time1))
timeres.append(time.time() - start_time1)


#PART 2
#Q1 using SQL with CSV 

start_time2 = time.time()

taxidata = sqlContext.read.csv("hdfs://master:9000/user/user/data/yellow_tripdata_1m.csv")
taxidata.withColumn('duration',(to_timestamp(col('_c2')) \
    .cast(LongType()) - to_timestamp(col('_c1')).cast(LongType()))/60 ) \
    .withColumn('start', substring(col("_c1"), 12, 2)).groupBy("start").avg("duration").sort(asc("start")).show(25)

print("--- %s seconds for Q1 using SQL with csv ---" % (time.time() - start_time2))
timeres.append(time.time() - start_time2)



#PART 3
#Q1 using SQL with Parquet 
#Read
start_time3 = time.time()
#taxidata.write.parquet("hdfs://master:9000/user/user/data/yellow_tripdata_1m.parquet")
taxidata = sqlContext.read.parquet("hdfs://master:9000/user/user/data/yellow_tripdata_1m.parquet")


taxidata.withColumn('duration',(to_timestamp(col('_c2')).cast(LongType()) - to_timestamp(col('_c1')).cast(LongType()))/60 ) \
  .withColumn('start', substring(col("_c1"), 12, 2))\
  .groupBy("start").avg("duration").sort(asc("start")).show(25)

print("--- %s seconds for Q1 using SQL with parquet ---" % (time.time() - start_time2))
timeres.append(time.time() - start_time3)




#------------PLOTING TIMES AND SAVE RESULT TO Q1.png ----------------#
objects = ('RDD', 'SQL using csv', 'SQL using parquet')
y_pos = np.arange(len(objects))
performance = timeres #edw einai h LISTA ME TOUS XRONOUS
plt.barh(y_pos, performance, align='center', alpha=0.5)
plt.yticks(y_pos, objects)
plt.xlabel('Time (s)')
plt.title('Time needed for Q1')
plt.show()
plt.savefig('Q1.png')