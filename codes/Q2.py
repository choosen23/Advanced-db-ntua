import matplotlib
matplotlib.use('Agg')

from pyspark import SparkConf, SparkContext
import pandas as pd
import os
import csv
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
import time
import math
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt






def haversine(f1,l1,f2,l2):
    a = math.sin((f2-f1)/2)**2+ math.cos(f1) * math.cos(f2) *math.sin((l2-l1)/2)**2
    c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))
    d = 6371 * c
    return d


def isNewYork(arg):
    line = arg.split(",")
    long1 = float(line[3])
    lat1 = float(line[4])
    long2 = float(line[5])
    lat2 = float(line[6])
    if (-80 < long1 < -70):
            if (-80 < long2 < -70):
                if (40 < lat1 < 46):
                    if (40 < lat2 < 46):
                        return True
    else:  
        return False




def removeShit(arg):
    if (arg[1] <= 0 ): return False
    else: return True


def validDate(arg):
    arg = str(arg)
    line = arg.split(",")
    lines = line[1]
    month = int(lines.split(' ')[0].split('-')[1])
    day = int(lines.split(' ')[0].split('-')[2])
    if (day >= 10 ):    
        return True
    return False
                
# Function to return the velocity of a trip (Km/h)
def getVelocity(arg):
        arg = str(arg)
        line = arg.split(",")
        # get the id of trip
        id1 = str(line[0])   
        datetime_object1 = datetime.strptime(line[1].split(" ")[-1], '%H:%M:%S')  #arg1 of strpite is start Time of trip
        datetime_object2 = datetime.strptime(line[2].split(" ")[-1], '%H:%M:%S')  #arg1 of strtype is finish Time of trip
        time = ((datetime_object2 - datetime_object1).total_seconds())
        timeToHour = time / 3600 #from sec -> hour
        distanceToKm = haversine(float(line[3]),float(line[4]),float(line[5]),float(line[6]))
        try: 
            return  ( id1 , distanceToKm / timeToHour )
        except:
            return  ( id1 , 0 )
         

def isTopFive(arg,lista):
    for i in lista:
        if ( arg[0] == i[0] ):
            return True      
    return False        
            
            
start_time = time.time()
conf = SparkConf().setAppName("MapReduce approach")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
timeres = []

df1 = sc.textFile("hdfs://master:9000/user/user/data/yellow_tripdata_1m.csv")
df2 = sc.textFile("hdfs://master:9000/user/user/data/yellow_tripvendors_1m.csv")

# Q2 : Average Trip Duration by hour of Starting Time of Trip 
# Start with Map
df1 = df1.filter(lambda line : validDate(line)) \
    .filter(lambda line : isNewYork(line)) \
    .map(lambda line : getVelocity(line)) \
    .top(5, key=lambda x: x[1])

res2 = df2.map(lambda s: s.split(",")) \
    .map(lambda line: (line[0],line[1])) \
    .filter(lambda line : isTopFive(line,df1)) \


res1 = sc.parallelize(df1)

result = res1.join(res2).collect()

print "-----------Q2 RESULTS-------------------"
print " "
for i in result:
    print(i[1])    

print "--------------------------"
print("------ %s seconds  ---" % (time.time() - start_time))
print "--------------------------"
timeres.append(time.time() - start_time)


#----------------------------------------------------------------#
#Q2 : using SQL with csv
start_time2 = time.time()
df1 = sqlContext.read.csv("hdfs://master:9000/user/user/data/yellow_tripdata_1m.csv")
df2 = sqlContext.read.csv("hdfs://master:9000/user/user/data/yellow_tripvendors_1m.csv")

df1.filter((substring(col("_c1"), 9, 2)>=10))\
    .withColumn("a", pow(sin((col("_c6")-col("_c4"))/2),2)+ cos(col("_c4")) * cos(col("_c6")) *pow(sin((col("_c5")-col("_c3"))/2),2) )\
    .withColumn("c",  2 * atan2(sqrt(col("a")),sqrt(1-col("a"))))\
    .withColumn("distance", col("c") * 6371)\
    .withColumn('duration',(to_timestamp(col('_c2')).cast(LongType()) - to_timestamp(col('_c1')).cast(LongType())) )\
    .withColumn("velocity", col("distance")/col("duration")*3.6)\
    .sort(col("velocity").desc())\
    .select("velocity","_c0").limit(5).join(df2, df2._c0 == df1._c0)\
    .withColumn("Vendor", col("_c1")).select("velocity", "Vendor").sort(col("velocity").desc())\
    .show()
    

print("--- %s seconds using csv  ------------" % (time.time() - start_time2))
timeres.append(time.time() - start_time2)
#----------------------------------------------------------------#
#Q3 : using SQL with parquet
#TIme - Write Parquet - Load Parquet
start_time3 = time.time()
# df1.write.parquet("hdfs://master:9000/yellow_tripdata_1m.parquet")
# df2.write.parquet("hdfs://master:9000/yellow_tripvendors_1m.parquet")

df1_parquet = sqlContext.read.parquet("hdfs://master:9000/yellow_tripdata_1m.parquet")
df2_parquet = sqlContext.read.parquet("hdfs://master:9000/yellow_tripvendors_1m.parquet")

#Query
df1_parquet.filter((substring(col("_c1"), 9, 2)>=10))\
    .withColumn("a", pow(sin((col("_c6")-col("_c4"))/2),2)+ cos(col("_c4")) * cos(col("_c6")) *pow(sin((col("_c5")-col("_c3"))/2),2) )\
    .withColumn("c",  2 * atan2(sqrt(col("a")),sqrt(1-col("a"))))\
    .withColumn("distance", col("c") * 6371)\
    .withColumn('duration',(to_timestamp(col('_c2')).cast(LongType()) - to_timestamp(col('_c1')).cast(LongType())) )\
    .withColumn("velocity", col("distance")/col("duration")*3.6)\
    .sort(col("velocity").desc())\
    .select("velocity","_c0").limit(5).join(df2_parquet, df2_parquet._c0 == df1_parquet._c0)\
    .withColumn("Vendor", col("_c1")).select("velocity", "Vendor").sort(col("velocity").desc())\
    .show()
    
    
    
print "--------------------------" 
print("--- %s seconds using parquet  -----------" % (time.time() - start_time3))
print "--------------------------"
timeres.append(time.time() - start_time3)



#------------PLOTING TIMES AND SAVE RESULT TO Q1.png ----------------#
objects = ('RDD', 'SQL using csv', 'SQL using parquet')
y_pos = np.arange(len(objects))
performance = timeres #edw einai h LISTA ME TOUS XRONOUS

plt.barh(y_pos, performance, align='center', alpha=0.5)
plt.yticks(y_pos, objects)
plt.xlabel('Time (s)')
plt.title('Time needed for Q2')
plt.show()
plt.savefig('Q2.png')