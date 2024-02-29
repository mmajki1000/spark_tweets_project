from pyspark.sql import SparkSession
from pyspark.sql.functions import lit



spark = SparkSession.builder\
                    .appName('cyce')\
                    .getOrCreate()
                    
def loader(file, name):
    df = spark.read\
            .option('header',True)\
            .csv(f"./data/{file}.csv")\
            .withColumn('name',lit(f"{name}"))\
            .na.drop()
    return df


def load_all():
    covid_df = loader('covid19_tweets','covid')
    financial_df = loader('financial','financial')
    grammys_df = loader('grammys_tweets','grammys')
    
    df = covid_df.unionByName(financial_df, allowMissingColumns = True)\
                .unionByName(grammys_df, allowMissingColumns = True)
    
    return df
    
     
# load_all()

            
