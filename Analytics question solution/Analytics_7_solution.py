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





# In[5]:


units=DFrame.Dataframe(getpath.get_path_from_config('input_path','units_input_path'))
damages=DFrame.Dataframe(getpath.get_path_from_config('input_path','damages_input_path'))


# In[ ]:





# # Create class for analytics 7 solution

# In[ ]:





# In[9]:


class Analytics_7_Solution:
    '''This class gives final result for analytics7'''
    
    def analytics7(self):
        '''7.	Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
                Approach: left join units with damages and filter on no damages and VEH_DMAG_SCL>4'''
        
        try:
            analytics7_final_df=units.df.join(damages.df,[units.df.CRASH_ID==damages.df.CRASH_ID],'left')                                .where("Damaged_property is null AND                                         split(VEH_DMAG_SCL_1_ID,' ')[1]>4 AND                                         split(VEH_DMAG_SCL_2_ID,' ')[1]>4 AND                                         FIN_RESP_TYPE_ID like '%INSURANCE%'")                                .select(units.df.CRASH_ID,"VEH_DMAG_SCL_1_ID","VEH_DMAG_SCL_2_ID","FIN_RESP_TYPE_ID")                                .agg(countDistinct(units.df.CRASH_ID).alias("Distinct_CrashID_Count"))
            analytics7_final_df.show(truncate=False)
            out=wDFrame.Write_Dataframe(analytics7_final_df,'analytics7_output_path','csv',getpath.get_path_from_config('output_path','analytics7_output_path'))
            out.write()
        
        except Exception as e:
            print(e)


# In[ ]:




