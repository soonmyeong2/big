#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[2]:


file_path = '/home/hadoop/'
file_name = file_path + 'airbnb.csv'
df_airbnb = pd.read_csv(file_name)


# In[3]:


df_air = df_airbnb.set_index(['price'])
accommo = df_air.groupby(['price'], as_index=True)['accommodates'].mean()
bedroom = df_air.groupby(['price'], as_index=True)['bedrooms'].mean()


# In[4]:


result = accommo.to_frame().merge(bedroom.to_frame(), right_index=True, left_on='price')
result.columns = ['accommodates average','bedrooms average']


# In[5]:


result.plot()
plt.xlabel("price")
plt.ylabel("rooms/days")
plt.show()


# In[54]:


sub_xy = df_airbnb.set_index(['neighborhood'])
sub_y1 = sub_xy.groupby(['neighborhood'], as_index=True)['reviews'].mean()
sub_y2 = sub_xy.groupby(['neighborhood'], as_index=True)['overall_satisfaction'].mean()
sub_y3 = sub_xy.groupby(['neighborhood'], as_index=True)['price'].mean()

fig, ax = plt.subplots(figsize=(13, 8))
plt.subplot(3,1,1)
plt.plot(sub_y1, marker = '*')
plt.xlabel("neighborhood")
plt.ylabel("reviews")
fig.autofmt_xdate()
plt.tight_layout()

fig1, ax1 = plt.subplots(figsize=(13, 8))
plt.subplot(3,1,2)
plt.plot(sub_y2, marker = '*')
plt.xlabel("neighborhood")
plt.ylabel("overall_satisfaction")
fig1.autofmt_xdate()
plt.tight_layout()

fig2, ax2 = plt.subplots(figsize=(13, 8))
plt.subplot(3,1,3)
plt.plot(sub_y3, marker = '*')
plt.xlabel("neighborhood")
plt.ylabel("price")
fig2.autofmt_xdate()
plt.tight_layout()

plt.show()

