import csv

from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName("football").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


fifa_data_csv = spark.read.format('csv').options(header='true', inferSchema='true',encoding='utf-8',delimiter=',',lineterminator='\n', quotechar='"').load("C:/Users/Documents/spark-practice/assignment/data.csv")

#fifa_data_csv.columns

new_column_names = [c.replace(' ', '_') for c in fifa_data_csv.columns]
fifa_data_csv = fifa_data_csv.toDF(*new_column_names)
#fifa_data_csv.show()

#1.Explain what you considered and the reason for your choice.
#storing FIFA19 data as parquet format becasue it has columnar storage structure it stores data in compressed format and gives effecient result when querying particualr columns and also aggregations
#partitioning of data is done on club as that is the most frequently quried fields so it helps predicate pushdown and avoids reading entire table, instead reads only required club and postion inside club
#bucketing is done on position and age to evenly distribute data on files using internal has algorithm which works well with int columns, and when queries for position, age with filter it can skip reading some files as the records are bucketed and stored over files

fifa_data_parquet = fifa_data_csv.write.partitionBy('club').bucketBy(4,'position','Age').saveAsTable('output',format='parquet',mode='overwrite',path="C:/Users/Documents/spark-practice/assignment/output/")


fifa_data = spark.read.format('parquet').options(header='true').load("C:/Users/Documents/spark-practice/assignment/output")
#fifa_data.printSchema()


fifa_data.createOrReplaceTempView("FOOTBALL")
#a. Which club has the most number of left footed midfielders under 30 years of age? 

sql1 = spark.sql("select club,Preferred_Foot,count(*) as cnt from FOOTBALL where Preferred_Foot='Left' and age < 30 and club is not null group by club,Preferred_Foot order by cnt desc limit 1 ")

sql1.show(truncate=False)


def split_col(col):
	return (int(col.split('+')[0])+int(col.split('+')[1]))
	
udf_split_col = udf(lambda x: split_col(x) if not x is None else 0)

#b. The strongest team by overall rating for a 4-4-2 formation
four_four_two = fifa_data.select(col("club"), col("CB"),col("RCB"), col("LCB"), col("LB"), (udf_split_col(col("CB"))+udf_split_col(col("RCB"))+udf_split_col(col("LCB"))+udf_split_col(col("LB"))).alias("FOUR_DEFENDDERS"), (udf_split_col(col("RM"))+udf_split_col(col("LCM"))+udf_split_col(col("RCM"))+udf_split_col(col("LM"))).alias("FOUR_MID_FIELDERS"), (udf_split_col(col("RF"))+udf_split_col(col("ST"))).alias("TWO_FORWARDS"))



four_four_two.createOrReplaceTempView("FFT_RATING")

sql2 = spark.sql("select club, max(FOUR_DEFENDDERS+FOUR_MID_FIELDERS+TWO_FORWARDS) as max_rating from FFT_RATING Group by club order by max_rating desc limit 1")

sql2.show(truncate=False)


#c. Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ? 
#expensive squad doesn't mean largest wage bill

fifa_data.createOrReplaceTempView("VALUE_WAGE")


sql3 = spark.sql("""select club, max(value) as max_value from (select club, 
sum(case when right(regexp_replace(value,'K',''),1)='M' THEN cast(regexp_replace(regexp_replace(value,'K',''),'M','') as double)*1000
else cast(regexp_replace(value,'K','') as double) END) AS value,
sum(regexp_replace(wage,'K','')) AS wage 
from VALUE_WAGE  where club is not null 
group by club order by club asc) group by club order by max_value limit 1 """)

sql3.show(truncate=False)


#d. Which position pays the highest wage in average? 

sql4 = spark.sql("""select position,avg(cast(coalesce(regexp_replace(wage,'K',''),0.0) as double)) as avg_wage from VALUE_WAGE where club is not null  group by position order by avg_wage desc limit 1 """)

sql4.show(truncate=False) 


#e. What makes a goalkeeper great? Share 4 attributes which are most relevant to becoming a good goalkeeper? 
sql5 = spark.sql("""select name,position,cast(height as string) as height, GKReflexes,GKPositioning, GKHandling from VALUE_WAGE 
where position='GK' and cast(height as string) like '6%' and GKReflexes > '60' and GKPositioning > '55' and GKHandling > '60' """)

sql5.show(truncate=False)

#f. What makes a good Striker (ST)? Share 5 attributes which are most relevant to becoming a top striker ? 

sql6 = spark.sql("""select name,position,LongShots,ballcontrol, positioning,finishing,reactions from VALUE_WAGE 
where position='ST' and LongShots > '55' and ballcontrol > '60' and positioning > '50' and finishing > '55' and reactions >'55' """)

sql6.show(truncate=False)

