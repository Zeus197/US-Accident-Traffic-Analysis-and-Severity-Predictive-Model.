from pyspark import SparkContext as sc 
from pyspark.sql import functions as func
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import when
from pyspark.sql.functions import hour, month, year
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
from pyspark.sql import DataFrameStatFunctions as sf
from pyspark.sql.window import Window


accident_df = spark.read.format('csv').options(header='true', inferschema = 'true').load("/user/it732/output.csv")

#Accidents per state (bigger states more cars hence more accidents)
df1 = accident_df.groupBy('State').count().select('State', func.col('count').alias('State_Count'))
df1 = df1.sort('State_Count',ascending=False)
df1.write.csv("State_Count.csv", header=True)

#Accident Severity per state for level 2 CA first then TX then FL for 3 CA first then TX then FL
df2 = accident_df.groupBy('State', 'Severity').count().select('State', 'Severity', func.col('count').alias('State_Severity_Count'))
df2 = df2.sort('State_Severity_Count',ascending=False)
df2.write.csv("State_Severity_Count.csv", header=True)

#Accident Severity due to wind speed
df3 = accident_df.select(col('Wind_Speed(mph)').alias("Wind_Speed_mph"), 'Severity')
magic_percentile = func.expr('percentile_approx(Wind_Speed_mph, 0.5)')
quantile1 = func.expr('percentile_approx(Wind_Speed_mph, array(0.25,0.5,0.75))')
df31 = df3.groupBy('Severity').agg(magic_percentile.alias('med_val_WindSpeed'))

df31.write.csv("median_val_WindSpeed.csv", header=True)

df33 = df3.groupBy('Severity').agg(quantile1.alias('quantile_val_WindSpeed'))
df33.write.csv("quantile_val_WindSpeed.csv", header=True)


#Accidents Severity and WindChill
df5 = accident_df.select(col('Wind_Chill(F)').alias('Wind_chill_F'), 'Severity')
df5 = df5.groupBy('Severity').agg(func.round(func.mean("Wind_chill_F"),2).alias('WindChillmean'))
df5.write.csv("WindChillmean.csv", header=True)

#Count of accidents by hour
df4 = accident_df.groupBy(hour('Start_Time').alias('Hour')).count().select('Hour', func.col('count').alias('Hour_Count'))
df4.write.csv("Hour_Count.csv", header=True)
#Severity of accidents by hour 
df6 = accident_df.groupBy(hour('Start_Time').alias('Hour'), 'Severity').count().select('Hour', 'Severity', func.col('count').alias('Hour_Severity_Count'))
df6.write.csv("Hour_Severity_Count.csv", header=True)

#Count of accidents by month Result(Count is Highest in October closely followed by december and november and count is lowest in february)
df7 = accident_df.groupBy(month('Start_Time').alias('Month')).count().select('Month', func.col('count').alias('Month_Count'))
df77 = df7.sort('Month_Count', ascending=False)
df77.write.csv("Month_Count.csv", header=True)

#Severity of accidents by month Result(Count is Highest in the month of October followed by december and november for severity level 2, August has most cases for severity level 3followed by october and december has most cases of severity level 4 followed by july)
df8 = accident_df.groupBy(month('Start_Time').alias('Month'), 'Severity').count().select('Month', 'Severity', func.col('count').alias('Month_Severity_Count'))
df88 = df8.sort('Month_Severity_Count', ascending=False)
#Percentage of total accidents by State (Find that california's makes 22 percent of total accident followed by Texas) and 50% of all accidents occor in the top 5 states
df9 = accident_df.groupBy('State').count().withColumn('percentage', func.round(100*(func.round(func.col('count') / func.sum('count').over(Window.partitionBy()),3))))
df10 = df9.sort('percentage', ascending=False)
df10.write.csv("State_percentage.csv", header=True)

#Accident count per  city Result(Houston TX has the most number of accidents followed by Charlotte NC and then Los Angeles CA moreover 3 of the top 5 states are in Texas)
df11 = accident_df.groupBy('State','City').count().select('State','City', func.col('count').alias('City_Count'))
df12 = df11.sort('City_Count', ascending=False)
df12.write.csv("City_Count.csv", header=True)

#Accident severity per City Result(For severity level '2' Houston is the City with the most number of accidents , Charloote comes in second Austin comes in third , for severity level 3 Los_Angeles has the most number of accidents, we also get to know that severlty level 2 is more common than severity level 3 with 4)

df13 = accident_df.groupBy('State', 'City','Severity').count().select('State', 'City','Severity', func.col('count').alias('City_Severity_Count'))
df13 = df13.sort('City_Severity_Count', ascending=False)
df13.write.csv("City_Severity_Count.csv", header=True)

#Accident Severity count and percentage (67% of total cases are of level 2 followed by level 3 at 29.8 percent and level 4 at 3.1 % and almost negligible percent in of level 1)
df14 = accident_df.groupBy('Severity').count().select('Severity', func.col('count').alias('Severity_Count'))
df15 = accident_df.groupBy('Severity').count().withColumn('percentage', func.round(100*(func.round(func.col('count') / func.sum('count').over(Window.partitionBy()),3)),4))
df15 = df15.sort('percentage', ascending=False)
df15.write.csv("Severity_percentage.csv", header=True)

#time duration
time_dif = ((func.unix_timestamp('End_Time') - func.unix_timestamp('Start_Time'))/3600)
df16 = accident_df.withColumn("Duration_hr", func.round(time_dif,2))
df21 = df16.groupBy("Severity").agg(func.round(func.mean('Duration_hr'),2).alias('mean_duration_by_severity'))
df21.write.csv('mean_duration_of_wrt_severity', header = True)

#
df18 = df16.withColumn("Day_of_week", func.dayofweek('Start_Time'))
df19 = df18.withColumn('Day_of_month', func.dayofmonth('Start_Time'))

# Accidents per week day

df20 = df19.groupBy('Day_of_week').count().select('Day_of_week', func.col('count').alias('accidents_per_weekday'))
for i in range(1,8):
	days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday' ,'']
	df20 = df20.withColumn("Day_of_week", \
         when(df20["Day_of_week"] == i, days[i-1]).otherwise(df20["Day_of_week"]))
	i = i+1
#
df19 = df19.groupBy('Day_of_month').count().select('Day_of_month', func.col('count').alias('accidents_per_day_of_month'))
#Average delay with respect to severity
df20.write.csv('day_of_week1.csv', header = True)