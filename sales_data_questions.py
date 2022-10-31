#!/usr/bin/env python
# coding: utf-8

# In[3]:


#Import all necessary libraries
from pyspark.sql import SparkSession


# In[4]:


# Creating a SparkSession in Python
spark = SparkSession.builder.appName('sales')    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar')    .getOrCreate()


# In[5]:


# Read json file
sales_data = spark.read.json('data/sales_records.json')    

sales_data.show()


# In[8]:


sales_data.printSchema()


# 1. Find the total cost, total revenue, total profit on the basis of each region.

# In[29]:


from pyspark.sql import SparkSession , functions as fun, Window as Wd
sales_data.groupBy('Region').agg(fun.sum('Total Cost'), fun.sum('Total Revenue'), fun.sum('Total Profit')).show()


# 2. Find the Item List on the basis of each country.

# In[30]:


sales_data.withColumn('Itemlist', fun.collect_set('Item Type').over(Wd.partitionBy('Country'))).select('Country', 'Itemlist').distinct().show()


# 3. Find the total number of items sold in each country.

# In[31]:


sales_data.groupBy('Country').agg(fun.sum('Units Sold').alias('Total number of items sold')).show()


# 4.  Find the top five famous items list on the basis of each region.(Consider units sold while doing this.)

# In[32]:


famous_item_in_region = sales_data.groupBy('Region', 'Item Type').agg(fun.sum('Units Sold').alias('Total Units Sold')).orderBy('Region', 'Total Units Sold')
famous_item_in_region.withColumn('rank', fun.rank().over(Wd.partitionBy('Region').orderBy(fun.col('Total Units Sold').desc()))).where('rank <= 5').show()


# 5. Find all the regions and their famous sales channels.

# In[34]:


sales_data.withColumn('Sales Channels', fun.collect_set('Sales Channel').over(Wd.partitionBy('Region'))).select('Region', 'Sales Channels').distinct().show()


# 6. Find the list of countries and items and their respective units.

# In[35]:


sales_data.groupBy('Country', 'Item Type').agg(fun.sum('Units Sold').alias('Units Sold')).orderBy('Country').show()


# 7. In 2013, identify the regions which sold maximum and minimum units of item type Meat.

# In[37]:


sales_2013=sales_data.withColumn('year', fun.substring('Order Date', -4, 4)).where(fun.col('year') == '2013')

sales_2013.where(sales_data['Item Type'] == 'Meat').groupBy('Region', 'Item Type').agg(F.sum('Units Sold').alias('Units Sold')).orderBy('Units Sold').show()


# 8. List all the items whose unit cost is less than 500.

# In[33]:


sales_data.filter(sales_data["Unit Cost"] < 500).select('Item Type', 'Unit Cost').distinct().show()


# 9. Find the total cost, revenue and profit of each year.

# In[38]:


sales_1 = sales_data.withColumn('Year', fun.substring('Order Date', -4, 4))
sales_1.groupBy('Year').agg(fun.sum('Total Cost'), fun.sum('Total Revenue'), fun.sum('Total Profit')).orderBy('Year').show()

