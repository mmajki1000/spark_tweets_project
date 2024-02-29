from loader import load_all
from pyspark.sql.functions import DataFrame,col, regexp_replace,split, explode



def cleaner(df:DataFrame):
       df = df.withColumn('hashtags',regexp_replace(col('hashtags'),"[\\[\\]']",""))\
           .withColumn('date',col('date').cast('date'))\
           .withColumn('user_created',col('Date').cast('date'))\
           .withColumn('user_favourites',col('Date').cast('date'))\
           .withColumn('user_friends',col('Date').cast('long'))\
           .withColumn('user_followers',col('Date').cast('long'))
       return df


