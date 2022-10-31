#!/usr/bin/env python
# coding: utf-8

# In[13]:


#Import all necessary libraries
import pandas as pd


# In[22]:


from pyspark.sql import SparkSession


# In[12]:


pip install pandas


# In[23]:


# Creating a SparkSession in Python
spark = SparkSession.builder.appName('sales')    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar')    .getOrCreate()


# In[24]:


# Read json file
sales_data = spark.read.json('data/sales_records.json')    

sales_data.show()


# In[25]:


sales_data.printSchema()


# 1. Find the total cost, total revenue, total profit on the basis of each region.

# In[26]:


from pyspark.sql import SparkSession , functions as fun, Window as Wd
total = sales_data.groupBy('Region').agg(fun.sum('Total Cost'), fun.sum('Total Revenue'), fun.sum('Total Profit'))
total.show()


# 2. Find the Item List on the basis of each country.

# In[30]:


each_country_item = sales_data.withColumn('Itemlist', fun.collect_set('Item Type').over(Wd.partitionBy('Country'))).select('Country', 'Itemlist').distinct()
each_country_item.show()


# In[31]:


each_country_item.toPandas().to_csv('output/sales/each_country_item.csv',index= False)


# 3. Find the total number of items sold in each country.

# In[33]:


total_no_item_sold = sales_data.groupBy('Country').agg(fun.sum('Units Sold').alias('Total number of items sold'))
total_no_item_sold.show()


# In[34]:


total_no_item_sold.toPandas().to_csv('output/sales/total_no_item_sold.csv',index= False)


# 4.  Find the top five famous items list on the basis of each region.(Consider units sold while doing this.)

# In[35]:


famous_item_in_region = sales_data.groupBy('Region', 'Item Type').agg(fun.sum('Units Sold').alias('Total Units Sold')).orderBy('Region', 'Total Units Sold')
famous_item_in_region.withColumn('rank', fun.rank().over(Wd.partitionBy('Region').orderBy(fun.col('Total Units Sold').desc()))).where('rank <= 5')
famous_item_in_region.show()


# In[36]:


famous_item_in_region.toPandas().to_csv('output/sales/famous_item_in_region.csv',index= False)


# 5. Find all the regions and their famous sales channels.

# In[37]:


famous_sale_channel_region = sales_data.withColumn('Sales Channels', fun.collect_set('Sales Channel').over(Wd.partitionBy('Region'))).select('Region', 'Sales Channels').distinct()
famous_sale_channel_region.show()


# In[38]:


famous_sale_channel_region.toPandas().to_csv('output/sales/famous_sale_channel_region.csv',index= False)


# 6. Find the list of countries and items and their respective units.

# In[39]:


countries_and_units = sales_data.groupBy('Country', 'Item Type').agg(fun.sum('Units Sold').alias('Units Sold')).orderBy('Country')
countries_and_units.show()


# In[40]:


countries_and_units.toPandas().to_csv('output/sales/countries_and_units.csv',index= False)


# 7. In 2013, identify the regions which sold maximum and minimum units of item type Meat.

# In[42]:


sales_2013=sales_data.withColumn('year', fun.substring('Order Date', -4, 4)).where(fun.col('year') == '2013')

max_mix_item = sales_2013.where(sales_data['Item Type'] == 'Meat').groupBy('Region', 'Item Type').agg(fun.sum('Units Sold').alias('Units Sold')).orderBy('Units Sold')
max_mix_item.show()


# In[43]:


max_mix_item.toPandas().to_csv('output/sales/max_mix_item.csv',index= False)


# 8. List all the items whose unit cost is less than 500.

# In[44]:


less_than_500 = sales_data.filter(sales_data["Unit Cost"] < 500).select('Item Type', 'Unit Cost').distinct()
less_than_500.show()


# In[45]:


less_than_500.toPandas().to_csv('output/sales/less_than_500.csv',index= False)


# 9. Find the total cost, revenue and profit of each year.

# In[46]:


sales_1 = sales_data.withColumn('Year', fun.substring('Order Date', -4, 4))
total_each_year = sales_1.groupBy('Year').agg(fun.sum('Total Cost'), fun.sum('Total Revenue'), fun.sum('Total Profit')).orderBy('Year')
total_each_year.show()


# In[47]:


total_each_year.toPandas().to_csv('output/sales/total_each_year.csv',index= False)

