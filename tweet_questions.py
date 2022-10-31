#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Import all necessary libraries
from pyspark.sql import SparkSession , functions as fun
from pyspark.sql.types import ArrayType, StringType


# In[2]:


# Creating a SparkSession in Python
spark = SparkSession.builder.appName('sales')    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar')    .getOrCreate()


# In[3]:


# Read json file
twitter_data = spark.read.json('data/tweets.json')    

twitter_data.show()


# In[4]:


twitter_data.printSchema()


# 1. Find all the tweets by user.

# In[6]:


user = 'hafizzul'

all_user_tweets = twitter_data.filter(twitter_data['user']==user)
all_user_tweets.show()


# In[7]:


all_user_tweets.toPandas().to_csv('output/tweet/all_user_tweets.csv',index= False)


# 2. Find how many tweets each user has.

# In[10]:


each_user_tweet = twitter_data.groupBy('user').count().orderBy('count', ascending=False)
each_user_tweet.show()


# In[11]:


each_user_tweet.toPandas().to_csv('output/tweet/each_user_tweet.csv',index= False)


# 3. Find all the persons mentioned on tweets.

# In[12]:


def generate_usermentioned(text):
    return [item.lstrip('@') for item in text.split(' ') if item.startswith('@')]
user_mentioned = twitter_data.withColumn('users_mentioned', fun.udf(lambda text: generate_usermentioned(text), ArrayType(StringType()))('text'))
user_mentioned.show()


# In[13]:


user_mentioned.toPandas().to_csv('output/tweet/user_mentioned.csv',index= False)


# 4. Count how many times each person is mentioned.

# In[11]:


from pyspark.sql.functions import explode


# In[14]:


new_usermentioned_df= user_mentioned.select(fun.explode('users_mentioned').alias('users_mentioned'))
new_usermentioned_df = new_usermentioned_df.filter(new_usermentioned_df['users_mentioned'] != '')
times_person_mentioned =new_usermentioned_df.groupBy('users_mentioned').count()
times_person_mentioned.show(truncate=False)


# In[15]:


times_person_mentioned.toPandas().to_csv('output/tweet/times_person_mentioned.csv',index= False)


# 5. Find the 10 most mentioned persons.

# In[19]:


ten_most_mentioned =new_usermentioned_df.groupBy('users_mentioned').count().orderBy('count', ascending=False).limit(10)
ten_most_mentioned.show()


# In[20]:


ten_most_mentioned.toPandas().to_csv('output/tweet/ten_most_mentioned.csv',index= False)


# 6. Find all the hashtags mentioned on a tweet.

# In[23]:


def generate_hashtags(text):
    return [item for item in text.split(' ') if item.startswith('#')]
hashtags_mentioned = twitter_data.withColumn('hashtags_mentioned', fun.udf(lambda text: generate_hashtags(text), ArrayType(StringType()))('text'))
hashtags_mentioned.show()


# In[24]:


hashtags_mentioned.toPandas().to_csv('output/tweet/hashtags_mentioned.csv',index= False)


# 7. Count how many times each hashtag is mentioned.

# In[32]:


new_hashtags_mentioned= hashtags_mentioned.select(fun.explode('hashtags_mentioned').alias('hashtags_mentioned'))
new_hashtags_mentioned = new_hashtags_mentioned.filter(new_hashtags_mentioned['hashtags_mentioned'] != '')
times_each_hashtag_mentioned=new_hashtags_mentioned.groupBy('hashtags_mentioned').count()
times_each_hashtag_mentioned.show(truncate=False)


# In[33]:


times_each_hashtag_mentioned.toPandas().to_csv('output/tweet/times_each_hashtag_mentioned.csv',index= False)


# 8. Find the 10 most popular Hashtags.

# In[35]:


ten_most_popular =new_hashtags_mentioned.groupBy('hashtags_mentioned').count().orderBy('count', ascending=False).limit(10)
ten_most_popular.show()


# In[36]:


ten_most_popular.toPandas().to_csv('output/tweet/ten_most_popular.csv',index= False)


# 9. Find the top 5 countries which tweet the most.

# In[38]:


top_5_tweets =twitter_data.groupBy('country').count().orderBy('count', ascending=False).limit(5)
top_5_tweets.show()


# In[39]:


top_5_tweets.toPandas().to_csv('output/tweet/top_5_tweets.csv',index= False)

