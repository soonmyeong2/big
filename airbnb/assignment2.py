#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[4]:


file_path = '/home/hadoop/'
file_name = file_path + 'airbnb.csv'
df_air = pd.read_csv(file_name)


# In[11]:


max_review = df_air.groupby(['neighborhood'], as_index=True)['reviews'].max()
min_review = df_air.groupby(['neighborhood'], as_index=True)['reviews'].min()
max_price = df_air.groupby(['neighborhood'], as_index=True)['price'].max()
min_price = df_air.groupby(['neighborhood'], as_index=True)['price'].min()


# In[23]:


df_neigh = df_air.groupby(['neighborhood'], as_index=True)[["reviews","overall_satisfaction","price"]].mean()

df_neigh = pd.merge(df_neigh, max_review.to_frame(), on='neighborhood')
df_neigh = pd.merge(df_neigh, min_review.to_frame(), on='neighborhood')
df_neigh = pd.merge(df_neigh, max_price.to_frame(), on='neighborhood')
df_neigh = pd.merge(df_neigh, min_price.to_frame(), on='neighborhood')
df_neigh.columns = ['avg of reviews', 'avg of overall_satisfaction', 'avg of price', 'max of reviews',
'min of reviews', 'max of price', 'min of price']


# In[24]:


save_file = "sorted_neighborhood_factors.csv"
df_neigh.to_csv(save_file)

