#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *


# In[ ]:





# In[5]:


spark = SparkSession.builder          .appName("BCG_Car_Crash_CaseStudy")          .getOrCreate()


# In[ ]:





# In[6]:


import Get_Path as getpath
import DataFrame_Create as DFrame
import Write_Dataframe as wDFrame


# In[ ]:





# # Create necessary dataframe

# In[ ]:





# In[7]:


person=DFrame.Dataframe(getpath.get_path_from_config('input_path','person_input_path'))


# In[ ]:





# # Create class for analytics 3 solution

# In[ ]:





# In[8]:


class Analytics_3_Solution:
    '''This class gives final result for analytics3'''
    def analytics3(self):
        '''3.	Analysis 3: Which state has highest number of accidents in which females are involved? .
           Approach: Filter all crash caused by female and group based on states. Count distinct crash and order by desc'''
        try:
            analytics3_final_df=person.df                                .filter(col('PRSN_GNDR_ID')=='FEMALE')                                .groupBy(col('DRVR_LIC_STATE_ID'))                                .agg(countDistinct(col('CRASH_ID')).alias("Count"))                                .orderBy(col('Count').desc())                                .limit(1)
            analytics3_final_df.show()
            out=wDFrame.Write_Dataframe(analytics3_final_df,'analytics3_output_path','csv',getpath.get_path_from_config('output_path','analytics3_output_path'))
            out.write()
        except Exception as e:
            print(e)   


# In[ ]:




