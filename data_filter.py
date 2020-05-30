from pyspark import SparkContext as sc 
from pyspark.sql import functions as func
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import when
from pyspark.sql.functions import hour
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce


#spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()


#Load csv file
accident_df = spark.read.format('csv').options(header='true', inferschema = 'true').load("/user/it732/US_Accidents_Dec19.csv")
#accident_df = spark.read.format('csv').options(header='true', inferschema = 'true').load("/user/it732/output.csv")
#count
#accident_df.count()
#Schema
#accident_df.printSchema()
#find total null values per column
#df1 = accident_df.agg(*[func.count(func.when(func.isnull(c),c)).alias(c) for c in accident_df.columns])
#df1.show()
#select weather related columns
#replace nan to 0 for selected columns
accident_df = accident_df.fillna(0, subset=['Humidity(%)', 'Precipitation(in)', 'Wind_Chill(F)', 'Wind_Speed(mph)','Visibility(mi)'])
#for temperature taking mean
df7 = accident_df.select("Temperature(F)").where(col("Temperature(F)").isNotNull())
mean7 = df7.agg({'Temperature(F)': 'mean'})
#mean7.show()
temp = 62.35
#for pressure taking mean
df8 = accident_df.select("Pressure(in)").where(col("Pressure(in)").isNotNull())
mean8 = df8.agg({'Pressure(in)': 'mean'})
#mean8.show()
pres = 29.83

accident_df = accident_df.fillna({'Temperature(F)': temp, 'Pressure(in)': pres})


# for these columns taking the value that occors max number of time
df9 = accident_df.select("Sunrise_Sunset").where(col("Sunrise_Sunset").isNotNull())
df10 = accident_df.select("Civil_Twilight").where(col("Civil_Twilight").isNotNull())
df11 = accident_df.select("Astronomical_Twilight").where(col("Astronomical_Twilight").isNotNull())
df12 = accident_df.select("Wind_Direction").where(col("Wind_Direction").isNotNull())
df13 = accident_df.select("Weather_Condition").where(col("Weather_Condition").isNotNull())

mod9 = df9.groupBy('Sunrise_Sunset').agg(func.count('Sunrise_Sunset'))
mod10 = df10.groupBy('Civil_Twilight').agg(func.count('Civil_Twilight'))
mod11 = df11.groupBy('Astronomical_Twilight').agg(func.count('Astronomical_Twilight'))
mod12 = df12.groupBy('Wind_Direction').agg(func.count('Wind_Direction'))
mod13 = df13.groupBy('Weather_Condition').agg(func.count('Weather_Condition'))

w_cond = 'Cloudy'
w_dir = 'Calm'
a_twi = 'Day'
c_twi = 'Day'
s_s = 'Day'

accident_df = accident_df.fillna({'Sunrise_Sunset': s_s, 'Civil_Twilight': c_twi, 'Astronomical_Twilight': a_twi, 'Wind_Direction': w_dir, 'Weather_Condition': w_cond})
df5 = accident_df.agg(*[func.count(func.when(func.isnull(c),c)).alias(c) for c in accident_df.columns])

#dropping end lat and lng
accident_df = accident_df.drop('End_Lat')
accident_df = accident_df.drop('End_Lng')

#filling default TCM value
accident_df = accident_df.fillna({'TMC': 201.0})

#filling na values of city with city that occurs the most in the state using sql
df14 = accident_df.select("State","City").where(col("City").isNotNull())
temp_table_name = "acc_us"
df14.createOrReplaceTempView(temp_table_name)
sql1 = "select State, City, Count(*) as count \
		from acc_us \
		group by State, City"
temp_table2 = 'state_city'
sql_gb = spark.sql(sql1)
sql_gb.createOrReplaceTempView(temp_table2)
sql2 = 'select State, max(count) as max_count from state_city group by State'
sql_gb2 = spark.sql(sql2)

temp_table3 = 'state_city_max'
sql_gb2.createOrReplaceTempView(temp_table3)
sql_gb.createOrReplaceTempView(temp_table2)

sql3 ='select state_city.State, state_city.City, state_city.count from state_city \
		inner join state_city_max on state_city.State = state_city_max.State and state_city.count = state_city_max.max_count'
sql33 = spark.sql(sql3)
temp_table5 = 'max_city'
sql33.createOrReplaceTempView(temp_table5)
temp_table4 = 'original'
accident_df.createOrReplaceTempView(temp_table4)
column_str = """`ID`,`Source`,`TMC`,`Severity`,`Start_Time`,`End_Time`,`Start_Lat`,`Start_Lng`,`Distance(mi)`,`Description`,`Number`,`Street`,`Side`,`County`,`Zipcode`,`Country`,`Timezone`,`Airport_Code`,`Weather_Timestamp`,`Temperature(F)`,`Wind_Chill(F)`,`Humidity(%)`,`Pressure(in)`,`Visibility(mi)`,`Wind_Direction`,`Wind_Speed(mph)`,`Precipitation(in)`,`Weather_Condition`,`Amenity`,`Bump`,`Crossing`,`Give_Way`,`Junction`,`No_Exit`,`Railway`,`Roundabout`,`Station`,`Stop`,`Traffic_Calming`,`Traffic_Signal`,`Turning_Loop`,`Sunrise_Sunset`,`Civil_Twilight`,`Nautical_Twilight`,`Astronomical_Twilight`"""
sql4 = """select """ + column_str + """,original.State, CASE 1 when original.City is null then max_city.City else original.City END AS City from original join max_city on original.State = max_city.State """
sql44 = spark.sql(sql4)

#filling na values of timezone with timezone that occurs the most in the state using sql
df22 = sql44.select("State","Timezone").where(col("Timezone").isNotNull())
temp_table_name22 = "acc_us22"
df22.createOrReplaceTempView(temp_table_name22)
sql22 = "select State, Timezone, Count(*) as count \
		from acc_us22 \
		group by State, Timezone"
temp_table23 = 'state_zone'
sql_gb23 = spark.sql(sql22)
sql_gb23.createOrReplaceTempView(temp_table23)
sql24 = 'select State, max(count) as max_count from state_zone group by State'
sql_gb24 = spark.sql(sql24)

temp_table25 = 'state_zone_max'
sql_gb24.createOrReplaceTempView(temp_table25)
sql_gb23.createOrReplaceTempView(temp_table23)

sql26 ='select state_zone.State, state_zone.Timezone, state_zone.count from state_zone \
		inner join state_zone_max on state_zone.State = state_zone_max.State and state_zone.count = state_zone_max.max_count'
sql_gb26 = spark.sql(sql26)
temp_table26 = 'max_zone'
sql_gb26.createOrReplaceTempView(temp_table26)
temp_table27 = 'original2'
sql44.createOrReplaceTempView(temp_table27)
column_str = """`ID`,`Source`,`TMC`,`Severity`,`Start_Time`,`End_Time`,`Start_Lat`,`Start_Lng`,`Distance(mi)`,`Description`,`Number`,`Street`,`Side`,`County`,`Zipcode`,`Country`,`City`,`Airport_Code`,`Weather_Timestamp`,`Temperature(F)`,`Wind_Chill(F)`,`Humidity(%)`,`Pressure(in)`,`Visibility(mi)`,`Wind_Direction`,`Wind_Speed(mph)`,`Precipitation(in)`,`Weather_Condition`,`Amenity`,`Bump`,`Crossing`,`Give_Way`,`Junction`,`No_Exit`,`Railway`,`Roundabout`,`Station`,`Stop`,`Traffic_Calming`,`Traffic_Signal`,`Turning_Loop`,`Sunrise_Sunset`,`Civil_Twilight`,`Nautical_Twilight`,`Astronomical_Twilight`"""
sql45 = """select """ + column_str + """,original2.State, CASE 1 when original2.Timezone is null then max_zone.Timezone else original2.Timezone END AS Timezone from original2 join max_zone on original2.State = max_zone.State """
sql_gb45 = spark.sql(sql45)

#adding missing description
sql_gb45 = sql_gb45.fillna({'Description': "Accident"})

#filling na values of zipcode with zipcode that occurs the most in the state using sql

df44 = sql_gb45.select("State","Zipcode").where(col("Zipcode").isNotNull())
temp_table_name_zip = "acc_us23"
df44.createOrReplaceTempView(temp_table_name_zip)
sql_zip = "select State, Zipcode, Count(*) as count \
		from acc_us23 \
		group by State, Zipcode"
temp_table_zip = 'state_code'
sql_gb_zip = spark.sql(sql_zip)
sql_gb_zip.createOrReplaceTempView(temp_table_zip)
sql_zip2 = 'select State, max(count) as max_count from state_code group by State'
sql_gb_zip2 = spark.sql(sql_zip2)

temp_table_zip3 = 'state_code_max'
sql_gb_zip2.createOrReplaceTempView(temp_table_zip3)
sql_gb_zip.createOrReplaceTempView(temp_table_zip)

sql26 ='select state_code.State, state_code.Zipcode, state_code.count from state_code \
		inner join state_code_max on state_code.State = state_code_max.State and state_code.count = state_code_max.max_count'
sql_gb26 = spark.sql(sql26)
temp_tablezip4 = 'max_code'
sql_gb26.createOrReplaceTempView(temp_tablezip4)
temp_tablezip5 = 'original3'
sql_gb45.createOrReplaceTempView(temp_tablezip5)
column_str = """`ID`,`Source`,`TMC`,`Severity`,`Start_Time`,`End_Time`,`Start_Lat`,`Start_Lng`,`Distance(mi)`,`Description`,`Number`,`Street`,`Side`,`County`,`Timezone`,`Country`,`City`,`Airport_Code`,`Weather_Timestamp`,`Temperature(F)`,`Wind_Chill(F)`,`Humidity(%)`,`Pressure(in)`,`Visibility(mi)`,`Wind_Direction`,`Wind_Speed(mph)`,`Precipitation(in)`,`Weather_Condition`,`Amenity`,`Bump`,`Crossing`,`Give_Way`,`Junction`,`No_Exit`,`Railway`,`Roundabout`,`Station`,`Stop`,`Traffic_Calming`,`Traffic_Signal`,`Turning_Loop`,`Sunrise_Sunset`,`Civil_Twilight`,`Nautical_Twilight`,`Astronomical_Twilight`"""
sql45 = """select """ + column_str + """,original3.State, CASE 1 when original3.Zipcode is null then max_code.Zipcode else original3.Zipcode END AS Zipcode from original3 join max_code on original3.State = max_code.State """
sql_gb45 = spark.sql(sql45)

#filling na values of Airport_code with Airport_code that occurs the most in the state using sql

df44 = sql_gb45.select("State","Airport_Code").where(col("Airport_Code").isNotNull())
temp_table_name_zip = "acc_us24"
df44.createOrReplaceTempView(temp_table_name_zip)
sql_zip = "select State, Airport_Code, Count(*) as count \
		from acc_us24 \
		group by State, Airport_Code"
temp_table_zip = 'state_air'
sql_gb_zip = spark.sql(sql_zip)
sql_gb_zip.createOrReplaceTempView(temp_table_zip)
sql_zip2 = 'select State, max(count) as max_count from state_air group by State'
sql_gb_zip2 = spark.sql(sql_zip2)

temp_table_zip3 = 'state_air_max'
sql_gb_zip2.createOrReplaceTempView(temp_table_zip3)
sql_gb_zip.createOrReplaceTempView(temp_table_zip)

sql26 ='select state_air.State, state_air.Airport_Code, state_air.count from state_air \
		inner join state_air_max on state_air.State = state_air_max.State and state_air.count = state_air_max.max_count'
sql_gb26 = spark.sql(sql26)
temp_tablezip4 = 'max_air'
sql_gb26.createOrReplaceTempView(temp_tablezip4)
temp_tablezip5 = 'original4'
sql_gb45.createOrReplaceTempView(temp_tablezip5)
column_str = """`ID`,`Source`,`TMC`,`Severity`,`Start_Time`,`End_Time`,`Start_Lat`,`Start_Lng`,`Distance(mi)`,`Description`,`Number`,`Street`,`Side`,`County`,`Timezone`,`Country`,`City`,`Zipcode`,`Weather_Timestamp`,`Temperature(F)`,`Wind_Chill(F)`,`Humidity(%)`,`Pressure(in)`,`Visibility(mi)`,`Wind_Direction`,`Wind_Speed(mph)`,`Precipitation(in)`,`Weather_Condition`,`Amenity`,`Bump`,`Crossing`,`Give_Way`,`Junction`,`No_Exit`,`Railway`,`Roundabout`,`Station`,`Stop`,`Traffic_Calming`,`Traffic_Signal`,`Turning_Loop`,`Sunrise_Sunset`,`Civil_Twilight`,`Nautical_Twilight`,`Astronomical_Twilight`"""
sql45 = """select """ + column_str + """,original4.State, CASE 1 when original4.Airport_Code is null then max_air.Airport_Code else original4.Airport_Code END AS Airport_Code from original4 join max_air on original4.State = max_air.State """
sql_gb45 = spark.sql(sql45)

#filling na values of Number with Number that occurs the most in the state using sql

df44 = sql_gb45.select("State","Number").where(col("Number").isNotNull())
temp_table_name_zip = "acc_us25"
df44.createOrReplaceTempView(temp_table_name_zip)
sql_zip = "select State, Number, Count(*) as count \
		from acc_us25 \
		group by State, Number"
temp_table_zip = 'state_num'
sql_gb_zip = spark.sql(sql_zip)
sql_gb_zip.createOrReplaceTempView(temp_table_zip)
sql_zip2 = 'select State, max(count) as max_count from state_num group by State'
sql_gb_zip2 = spark.sql(sql_zip2)

temp_table_zip3 = 'state_num_max'
sql_gb_zip2.createOrReplaceTempView(temp_table_zip3)
sql_gb_zip.createOrReplaceTempView(temp_table_zip)

sql26 ='select state_num.State, state_num.Number, state_num.count from state_num \
		inner join state_num_max on state_num.State = state_num_max.State and state_num.count = state_num_max.max_count'
sql_gb26 = spark.sql(sql26)

sql_gb26 = sql_gb26.where((sql_gb26.Number != 7000.0) & (sql_gb26.Number != 4791.0) & (sql_gb26.Number != 700.0) & (sql_gb26.Number != 1325.0) &(sql_gb26.Number != 2301.0) & (sql_gb26.Number != 6102.0) & (sql_gb26.Number != 6402.0) & (sql_gb26.Number != 6600.0) & (sql_gb26.Number != 274.0) & (sql_gb26.Number != 1938.0))


temp_tablezip4 = 'max_num'
sql_gb26.createOrReplaceTempView(temp_tablezip4)
temp_tablezip5 = 'original5'
sql_gb45.createOrReplaceTempView(temp_tablezip5)
column_str = """`ID`,`Source`,`TMC`,`Severity`,`Start_Time`,`End_Time`,`Start_Lat`,`Start_Lng`,`Distance(mi)`,`Description`,`Airport_Code`,`Street`,`Side`,`County`,`Timezone`,`Country`,`City`,`Zipcode`,`Weather_Timestamp`,`Temperature(F)`,`Wind_Chill(F)`,`Humidity(%)`,`Pressure(in)`,`Visibility(mi)`,`Wind_Direction`,`Wind_Speed(mph)`,`Precipitation(in)`,`Weather_Condition`,`Amenity`,`Bump`,`Crossing`,`Give_Way`,`Junction`,`No_Exit`,`Railway`,`Roundabout`,`Station`,`Stop`,`Traffic_Calming`,`Traffic_Signal`,`Turning_Loop`,`Sunrise_Sunset`,`Civil_Twilight`,`Nautical_Twilight`,`Astronomical_Twilight`"""
sql45 = """select """ + column_str + """,original5.State, CASE 1 when original5.Number is null then max_num.Number else original5.Number END AS Number from original5 join max_num on original5.State = max_num.State """
sql_gb45 = spark.sql(sql45)


#df_s = sql_gb45.withColumn("Weather_Timestamp", \
 #             when(sql_gb45["Weather_Timestamp"] == sql_gb45.where(col("Weather_Timestamp").isNull()), sql_gb45["Start_Time"]).otherwise(sql_gb45["Weather_Timestamp"]))


#nautical twilight
nt = sql_gb45.select("ID","Source","TMC","Severity","Start_Time","End_Time","Start_Lat","Start_Lng","Distance(mi)","Description","Airport_Code","State","Street","Side","County","Number","Timezone","Country","City","Zipcode","Weather_Timestamp","Temperature(F)","Wind_Chill(F)","Humidity(%)","Pressure(in)","Visibility(mi)","Wind_Direction","Wind_Speed(mph)","Precipitation(in)","Weather_Condition","Amenity","Bump","Crossing","Give_Way","Junction","No_Exit","Railway","Roundabout","Station","Stop","Traffic_Calming","Traffic_Signal","Turning_Loop","Sunrise_Sunset","Civil_Twilight","Nautical_Twilight","Astronomical_Twilight",hour('Start_Time').alias('Hour'))

temp_tablezip6 = 'original6'
nt.createOrReplaceTempView(temp_tablezip6)

column_str = """`ID`,`Source`,`TMC`,`Severity`,`Start_Time`,`End_Time`,`Start_Lat`,`Start_Lng`,`Distance(mi)`,`Description`,`Airport_Code`,`State`,`Street`,`Side`,`County`,`Timezone`,`Country`,`City`, `Number`,`Zipcode`,`Weather_Timestamp`,`Temperature(F)`,`Wind_Chill(F)`,`Humidity(%)`,`Pressure(in)`,`Visibility(mi)`,`Wind_Direction`,`Wind_Speed(mph)`,`Precipitation(in)`,`Weather_Condition`,`Amenity`,`Bump`,`Crossing`,`Give_Way`,`Junction`,`No_Exit`,`Railway`,`Roundabout`,`Station`,`Stop`,`Traffic_Calming`,`Traffic_Signal`,`Turning_Loop`,`Sunrise_Sunset`,`Civil_Twilight`,`Astronomical_Twilight`"""
sqlnt = """select """ + column_str + """, CASE 1 when original6.Nautical_Twilight is null then CASE 1 when original6.Hour > 6 AND original6.Hour < 18 then 'Day' else 'Night' END else original6.Nautical_Twilight END AS Nautical_Twilight from original6"""
sqlnt2 = spark.sql(sqlnt)

df_s = sqlnt2.withColumn("Weather_Timestamp",coalesce(sqlnt2.Weather_Timestamp,sqlnt2.Start_Time))
df5 = df_s.agg(*[func.count(func.when(func.isnull(c),c)).alias(c) for c in df_s.columns])
#df5.show() 

df_s.write.csv("us_accident19.csv")
#df.write.format("com.databricks.spark.csv").option("header", "true").save("mydata.csv")
