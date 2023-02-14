#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *


# In[ ]:





# In[2]:


spark = SparkSession.builder          .appName("BCG_Car_Crash_CaseStudy")          .getOrCreate()


# In[ ]:





# In[3]:


import Get_Path as getpath
import DataFrame_Create as DFrame
import Write_Dataframe as wDFrame


# In[ ]:





# # Create necessary dataframe

# In[ ]:





# In[4]:


person=DFrame.Dataframe(getpath.get_path_from_config('input_path','person_input_path'))


# In[ ]:





# # Create class for analytics 1 solution 

# In[ ]:





# In[ ]:


class Analytics_1_Solution:
    '''This class gives final result for analytics1'''
    def analytics1(self):
        '''.	Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
        Approach: In person_df, filter for male and injury severity as killed and count distinct crash id's'''
        try:
            analytics1_final_df=person.df                                .filter((col('PRSN_GNDR_ID')=='MALE') & (col('PRSN_INJRY_SEV_ID')=='KILLED'))                                .agg(countDistinct(col('CRASH_ID')).alias('Count'))
            analytics1_final_df.show()
            
            out=wDFrame.Write_Dataframe(analytics1_final_df,'analytics1_output_path','csv',getpath.get_path_from_config('output_path','analytics1_output_path'))
            out.write()
        except Exception as e:
            print(e)


# In[ ]:




