#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[3]:


file_path = '/home/hadoop/'
file_name = file_path + 'airbnb.csv'
df_air = pd.read_csv(file_name)


# In[155]:


df1 = df_air.groupby(pd.cut(df_air["price"], np.arange(0, 501, 100)))['accommodates'].mean()
df2 = df_air.groupby(pd.cut(df_air["price"], np.arange(0, 501, 100)))['accommodates'].median()
df3 = df_air.groupby(pd.cut(df_air["price"], np.arange(0, 501, 100)))['bedrooms'].mean()
df4 = df_air.groupby(pd.cut(df_air["price"], np.arange(0, 501, 100)))['bedrooms'].median()
df5 = df_air.groupby(pd.cut(df_air["price"], np.arange(0, 501, 100)))['reviews'].mean()
df6 = df_air.groupby(pd.cut(df_air["price"], np.arange(0, 501, 100)))['reviews'].median()
df7 = df_air.groupby(pd.cut(df_air["price"], np.arange(0, 501, 100)))['neighborhood'].apply(list)
df8 = df_air.groupby(pd.cut(df_air["price"], np.arange(0, 501, 100)))['reviews'].median()
for i,j in enumerate(df7):
    df7[i] = list(set(j))
    df8[i] = len(list(set(j)))
re1 = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8],axis=1)


#############################

df1 = df_air.groupby(pd.cut(df_air["price"], np.arange(500, 1001, 500)))['accommodates'].mean()
df2 = df_air.groupby(pd.cut(df_air["price"], np.arange(500, 1001, 500)))['accommodates'].median()
df3 = df_air.groupby(pd.cut(df_air["price"], np.arange(500, 1001, 500)))['bedrooms'].mean()
df4 = df_air.groupby(pd.cut(df_air["price"], np.arange(500, 1001, 500)))['bedrooms'].median()
df5 = df_air.groupby(pd.cut(df_air["price"], np.arange(500, 1001, 500)))['reviews'].mean()
df6 = df_air.groupby(pd.cut(df_air["price"], np.arange(500, 1001, 500)))['reviews'].median()
df7 = df_air.groupby(pd.cut(df_air["price"], np.arange(500, 1001, 500)))['neighborhood'].apply(list)
df8 = df_air.groupby(pd.cut(df_air["price"], np.arange(500, 1001, 500)))['reviews'].median()
for i,j in enumerate(df7):
    df7[i] = list(set(j))
    df8[i] = len(list(set(j)))
re2 = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8],axis=1)

#####################


df1 = df_air.groupby(pd.cut(df_air["price"], np.arange(1000, 5001, 4000)))['accommodates'].mean()
df2 = df_air.groupby(pd.cut(df_air["price"], np.arange(1000, 5001, 4000)))['accommodates'].median()
df3 = df_air.groupby(pd.cut(df_air["price"], np.arange(1000, 5001, 4000)))['bedrooms'].mean()
df4 = df_air.groupby(pd.cut(df_air["price"], np.arange(1000, 5001, 4000)))['bedrooms'].median()
df5 = df_air.groupby(pd.cut(df_air["price"], np.arange(1000, 5001, 4000)))['reviews'].mean()
df6 = df_air.groupby(pd.cut(df_air["price"], np.arange(1000, 5001, 4000)))['reviews'].median()
df7 = df_air.groupby(pd.cut(df_air["price"], np.arange(1000, 5001, 4000)))['neighborhood'].apply(list)
df8 = df_air.groupby(pd.cut(df_air["price"], np.arange(1000, 5001, 4000)))['reviews'].median()
for i,j in enumerate(df7):
    df7[i] = list(set(j))
    df8[i] = len(list(set(j)))
re3 = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8],axis=1)

#########################


# In[158]:


result = pd.concat([re1, re2, re3], ignore_index=True)
index_list = ['(0-100]', '(100-200]', '(200-300]', '(300-400]', '(400-500]', '(500-1000]', '(1000-5000]']
result.index.name = 'price'
result.index = index_list
result.columns = ['accommodates average', 'accommodates median', 'bedrooms average', 'bedrooms median', 'reviews average', 'reviews median','neightbor list', 'length']


# In[159]:


save_file = "sort_ranged_price.csv"
result.to_csv(save_file)

