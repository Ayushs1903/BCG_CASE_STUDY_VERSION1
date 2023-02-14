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
# 

# In[ ]:





# In[6]:


person=DFrame.Dataframe(getpath.get_path_from_config('input_path','person_input_path'))
units=DFrame.Dataframe(getpath.get_path_from_config('input_path','units_input_path'))


# In[ ]:





# # Create class for analytics 5 solution

# In[ ]:





# In[7]:



class Analytics_5_Solution:
    '''This class gives final result for analytics5'''
    def analytics5(self):
        '''5.	Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style 
           Approach: Join units df with person df to get ethinic group and aggregate based on distinct crashID and select top most record'''
        try:
            windowspec=Window.partitionBy(col('VEH_BODY_STYL_ID')).orderBy(col('Count').desc())
            analytics5_final_df= units.df.join(person.df, [units.df.CRASH_ID==person.df.CRASH_ID,units.df.UNIT_NBR==person.df.UNIT_NBR],'inner')                                .select(col('VEH_BODY_STYL_ID'),col('PRSN_ETHNICITY_ID'))                                .groupBy(col('VEH_BODY_STYL_ID'),col('PRSN_ETHNICITY_ID'))                                .agg(count('*').alias('Count'))                                .withColumn('RNK',row_number().over(windowspec))                                .filter(col('RNK')==1).select(col('VEH_BODY_STYL_ID'),col('PRSN_ETHNICITY_ID'))
            
            analytics5_final_df.show()
            out=wDFrame.Write_Dataframe(analytics5_final_df,'analytics5_output_path','csv',getpath.get_path_from_config('output_path','analytics5_output_path'))
            out.write()
        except Exception as e:
            print(e) 


# In[ ]:




