from pyspark.sql.functions import col, DataFrame,split, lit, array_intersect,size


def search_by_user_loc(df:DataFrame, location):
    df = df.filter(col('user_location').contains(f'{location}'))
    return df


def search_by_key_word(df:DataFrame, column, key_word):
    df = df.filter(col(f'{column}').contains(f'{key_word}'))
    return df


def search_by_key_words(df:DataFrame, key_words = []):
    
     df = df.withColumn('temp_match',array_intersect(split(col('text'),' '),lit(key_words)))\
            .filter((size('temp_match') == size(lit(key_words))))\
            .drop(col('temp_match'))
           
     return df