#!/usr/bin/env python
# coding: utf-8

# In[99]:


import pandas as pd
from sqlalchemy import create_engine
import datetime


# In[83]:


file_one = "2019_nCoV_data.csv"
file_one_df = pd.read_csv(file_one)
file_one_df.head()


# In[84]:


file_two = "covid_19_data.csv"
file_two_df = pd.read_csv(file_two)
file_two_df.head()


# In[85]:


confirmed = "time_series_covid_19_confirmed.csv"
confirmed_df = pd.read_csv(file_three)
clean_confirmed = confirmed_df.drop(['Lat', 'Long', 'Province/State','Country/Region'], axis=1)
clean_confirmed.loc['confirmed']= clean_confirmed.sum(numeric_only=True, axis=0)

confirmed_df = clean_confirmed.loc['confirmed'].to_frame()
confirmed_df.rename_axis('Date')


# In[86]:


deaths = "time_series_covid_19_deaths.csv"
deaths_df = pd.read_csv(deaths)
clean_deaths = deaths_df.drop(['Lat', 'Long', 'Province/State','Country/Region'], axis=1)
clean_deaths.loc['deaths']= clean_deaths.sum(numeric_only=True, axis=0)

death_df = clean_deaths.loc['deaths'].to_frame()
death_df.rename_axis('Date')


# In[87]:


recoveries = "time_series_covid_19_recovered.csv"
recovered_df = pd.read_csv(recoveries)
clean_recoveries = recovered_df.drop(['Lat', 'Long', 'Province/State','Country/Region'], axis=1)
clean_recoveries.loc['recoveries']= clean_recoveries.sum(numeric_only=True, axis=0)

#clean_deaths.loc['Recoveries'].to_frame()
recovery_df = clean_recoveries.loc["recoveries"].to_frame()
recovery_df.rename_axis('Date')


# In[88]:


death_join_confirmed = death_df.join(confirmed_df)
death_join_confirmed


# In[89]:


all_joined = death_join_confirmed.join(recovery_df)
final = all_joined.rename_axis('date')
final=final.reset_index()
final


# In[90]:


final.dtypes


# In[96]:


connection_string = "postgres:password@localhost:5432/etl_summary_table"
engine = create_engine(f'postgresql://{connection_string}')


# In[97]:


engine.table_names()


# In[98]:


final.to_sql(name = "corona_virus_summary", con=engine, if_exists='append', index=False)


# In[ ]:




