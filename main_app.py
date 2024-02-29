from loader import load_all
from cleaner import cleaner
from analyzer import tweet_counter, calculatehashtags, calc_followers_population
from searcher import search_by_key_word, search_by_user_loc, search_by_key_words


                                                ### loading ###

## this function:
# - loads 3 files from path './data/...' - covid19_tweets, financial, grammys_tweets
# - dropinng N/A
# - adding column in each df: covid, financial, grammys
# - doing unionByName of the 3 df's
 
df = load_all()


                                                ### cleaning ###

## cleaner requires 1 parameter - data frame for cleaning:
# - cast
# - hashtags split

clean_df = cleaner(df)
clean_df.cache()


                                                ### Searching ###

## searching df by any column and any key word
### available columns:

# availabuser_name
# user_location
# user_description
# user_creat
# user_followers
# user_frien
# user_favourit
# user_verified
# date
# text
# hashtags
# source
# is_retweet
# name:
# id
# timestamp
# symbols
# company_names
# url
# verified


## searching df by column 'user_location', returns df
### required df and location as parameters

df_uk_user_loc = search_by_user_loc(clean_df,'UK')


## searching df by any given column and key word, returns df:
### required df, column, key word as parameters

df_key_word_trump = search_by_key_word(clean_df, 'text', 'Trump')


## searching df and column 'text' by given key words, returns df:
### required df, key words as parameters

key_words = ['How','#COVID19']
df_by_kw = search_by_key_words(clean_df, key_words)




                                                 ### analyzing ###


## function for hastags calculation in 'hashtags' column for any df given:

count_hashtags = calculatehashtags(df_key_word_trump)


## function for tweets calculation for any df passed as parameters

count_tweets = tweet_counter(df_by_kw)



## function for avg user followers calculation for any df in a country passed as argument of column "user_location"

avg_followers = calc_followers_population(df_uk_user_loc)







