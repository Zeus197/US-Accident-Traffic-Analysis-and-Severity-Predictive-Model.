from pyspark import SparkContext as sc
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import col, hour, month, year
from pyspark.sql.types import FloatType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier as RF
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import OneHotEncoder

accident_df = spark.read.format('csv').options(header='true', inferschema = 'true').load("/user/it732/output.csv")
#df = accident_df.where(accident_df.State =='CA')

#getting year,month, day, hour, week day
def preprocessing(df):
    #time difference
    time_dif = ((func.unix_timestamp('End_Time') - func.unix_timestamp('Start_Time'))/3600)
    df = df.withColumn("Duration_hr", func.round(time_dif,2))
    #Removing negetive durations(Total 13)
    df = df.where(func.col("Duration_hr")>=0)
    df = df.withColumn("Hours", hour('Start_Time'))
    df = df.withColumn("Month", month('Start_Time'))
    df = df.withColumn("Year", year('Start_Time'))
    df = df.withColumn("Day_of_week", func.dayofweek('Start_Time'))
    df = df.withColumn('Day_of_month', func.dayofmonth('Start_Time'))
    
    df = df.dropna(how='any')
    df = df.withColumn("Distance(mi)", df["Distance(mi)"].cast(FloatType()))
    df = df.withColumn("Temperature(F)", df["Temperature(F)"].cast(FloatType()))
    df = df.withColumn("Humidity(%)", df["Humidity(%)"].cast(FloatType()))
    df = df.withColumn("Pressure(in)", df["Pressure(in)"].cast(FloatType()))
    df = df.withColumn("Visibility(mi)", df["Visibility(mi)"].cast(FloatType()))
    
    feature_lst=['Source','TMC','Severity','Distance(mi)','Side','State','Timezone','Temperature(F)','Humidity(%)','Pressure(in)','Visibility(mi)','Wind_Direction','Weather_Condition','Amenity','Bump','Crossing','Give_Way','Junction','No_Exit','Railway','Roundabout','Station','Stop','Traffic_Calming','Traffic_Signal','Turning_Loop','Sunrise_Sunset','Hours', 'Month', 'Year', 'Day_of_week', 'Day_of_month']
    df = df[feature_lst]
    #df = df.drop('State')  #Can remove if only using single state data
    #df = df.drop('Timezone')
    return df

def indexing(df):
    #Converting String values to indexes
    inputcols = ['Source','Side','Wind_Direction','Weather_Condition','Sunrise_Sunset','State','Timezone']
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in inputcols]
    pipeline = Pipeline(stages=indexers)
    df = pipeline.fit(df).transform(df)
    df = df.drop(*inputcols)
    return df

def transform(df):    
    cols = df.columns
    cols.remove('Severity')
    vecAssembler = VectorAssembler(inputCols=cols, outputCol="features")
    df_transformed = vecAssembler.transform(df)
    return df_transformed

def evaluate_model(df):
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol='Severity')
    accuracy_rf = evaluator.evaluate(df)
    return accuracy_rf

preprocessed_df = preprocessing(accident_df)
indexed_df = indexing(preprocessed_df)
transformed_df = transform(indexed_df)
#Split data into Training and Testing
train, test = transformed_df.randomSplit([0.7, 0.3], seed = 2000)
#Using Random Forest Algorithm
rf = RF(featuresCol='features',numTrees=12, maxDepth=16, labelCol="Severity",maxBins=150)
model_rf = rf.fit(train)
#Predicting on test data
prediction_rf = model_rf.transform(test)
accuracy = evaluate_model(prediction_rf)
print("Accuracy is ",accuracy)