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


units=DFrame.Dataframe(getpath.get_path_from_config('input_path','units_input_path'))


# In[ ]:





# # Create class for analytics 2 solution

# In[ ]:





# In[5]:


class Analytics_2_Solution:
    '''This class gives final result for analytics2'''
    def analytics2(self):
        '''2.	Analysis 2: How many two wheelers are booked for crashes? 
         Approach: Find VEH_BODY_STYL_ID as MOTORCYCLE and count distinct VIN as unique vehicle number'''
        
        try:
            analytics2_final_df= units.df                                .filter(col('VEH_BODY_STYL_ID')=='MOTORCYCLE')                                .select(col('VIN')).agg(countDistinct(col('VIN')).alias('count'))
            analytics2_final_df.show()
            
            out=wDFrame.Write_Dataframe(analytics2_final_df,'analytics2_output_path','csv',getpath.get_path_from_config('output_path','analytics2_output_path'))
            out.write()
        except Exception as e:
            print(e)    


# In[ ]:




