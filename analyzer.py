from cleaner import cleaner
from pyspark.sql.functions import explode_outer, col, DataFrame,split,isnotnull, desc



def calculatehashtags(df:DataFrame):
    df = df.withColumn('hashtags',split(col('hashtags'),','))\
           .withColumn('hashtags',explode_outer(col('hashtags')))\
           .groupBy('hashtags').count()\
           .sort(desc(col('count')))
    return df.show()




# def tweet_counter(df:DataFrame,group_column):
#     df = df.groupBy(f"{group_column}").count()\
#             .sort(desc(f'{group_column}'))
#     return df.show()



def tweet_counter(df:DataFrame):
    df = df.groupBy(col('text')).count()\
            .sort(desc(col('count')))
    return df.show()


def calc_followers_population(df:DataFrame):
    df = df.select(col('user_name'),col('user_location'),col('user_followers'))\
           .filter(isnotnull(col('user_name')))\
           .filter(isnotnull(col('user_location')))\
           .dropDuplicates(['user_name'])\
           .groupBy('user_location')\
           .avg('user_followers')\
           .sort(desc(col('avg(user_followers)')))
       
    return df.show()
        
# calc_followers_population(df)
# calculate_retweets = tweet_counter(df,'is_retweet')
# calculate_source = tweet_counter(df,'source')    
    