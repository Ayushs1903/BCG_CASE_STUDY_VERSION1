#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *


# In[ ]:





# In[7]:


spark = SparkSession.builder          .appName("BCG_Car_Crash_CaseStudy")          .getOrCreate()


# In[ ]:





# In[8]:


import Get_Path as getpath
import DataFrame_Create as DFrame
import Write_Dataframe as wDFrame


# In[ ]:





# # Create necessary dataframe
# 

# In[ ]:





# In[9]:


units=DFrame.Dataframe(getpath.get_path_from_config('input_path','units_input_path'))


# In[ ]:





# # Create class for analytics 4 solution

# In[ ]:





# In[10]:


class Analytics_4_Solution:
    '''This class gives final result for analytics4'''
    def analytics4(self):
        '''4.	Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
           Approach:  Group by VEH_MAKE_ID and count total injury including deaths and rank 5 to 15th'''
        try:
            windowspec=Window.orderBy(col('total').desc())
            analytics4_final_df=units.df.groupBy(col('VEH_MAKE_ID'))                                .agg((sum(col('TOT_INJRY_CNT'))+sum(col('DEATH_CNT'))).alias('total'))                                .withColumn('RNK',row_number().over(windowspec))                                .filter(col('RNK').between(5,15))
            analytics4_final_df.show()
            out=wDFrame.Write_Dataframe(analytics4_final_df,'analytics4_output_path','csv',getpath.get_path_from_config('output_path','analytics4_output_path'))
            out.write()
        except Exception as e:
            print(e)


# In[ ]:




