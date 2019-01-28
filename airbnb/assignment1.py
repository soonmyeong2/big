#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[2]:


file_path = '/home/hadoop/'
file_name = file_path + 'airbnb.csv'
df_air = pd.read_csv(file_name)
#fillna는 NaN값을 0으로 대체 함 연산해야 하므로 0으로 채움
df_air['overall_satisfaction'] = df_air.overall_satisfaction.fillna(0) 


# In[3]:


df_neigh = df_air.set_index(['room_id','host_id'])
df_neigh['total_score'] = pd.Series([])
overall = df_neigh.loc[:,'overall_satisfaction']
review = df_neigh.loc[:,'reviews']
df_neigh['total_score'] = overall + review * 0.378


# In[4]:


df_neigh = df_neigh.loc[:,['total_score']]

df_neigh = df_neigh.sort_values(["total_score"], ascending = [True]) # 오름차순
ascend_file = "./sorted_total_score_ascend.csv"
df_neigh.to_csv(ascend_file)

df_neigh = df_neigh.sort_values(["total_score"], ascending = [False]) # 내림차순
descend_file = "sorted_total_score_descend.csv"
df_neigh.to_csv(descend_file)

