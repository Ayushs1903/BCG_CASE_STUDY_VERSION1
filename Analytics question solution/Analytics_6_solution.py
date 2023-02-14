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
units=DFrame.Dataframe(getpath.get_path_from_config('input_path','units_input_path'))


# In[ ]:





# # Create class for analytics 6 solution

# In[ ]:





# In[5]:


class Analytics_6_Solution:
    '''This class gives final result for analytics6'''
    def analytics6(self):
        '''6.	Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
             Approach: Filter all cases of drinking from unitsv and join with person df to get drier zip codes'''
        try:
            alcohol_df=units.df.filter(col('CONTRIB_FACTR_1_ID').like('%ALCOHOL%') | 
                                       col('CONTRIB_FACTR_1_ID').like('%DRINKING%') |
                                       col('CONTRIB_FACTR_2_ID').like('%ALCOHOL%') |
                                       col('CONTRIB_FACTR_2_ID').like('%DRINKING%') |
                                       col('CONTRIB_FACTR_P1_ID').like('%ALCOHOL%') |
                                       col('CONTRIB_FACTR_P1_ID').like('%DRINKING%') ) \
                       .select(col('CRASH_ID'),col('CONTRIB_FACTR_1_ID'),col('CONTRIB_FACTR_2_ID'),col('CONTRIB_FACTR_P1_ID')) 
            
            analytics6_final_df=person.df.join(alcohol_df,[alcohol_df.CRASH_ID==person.df.CRASH_ID],'inner')                                .filter((col('PRSN_ALC_RSLT_ID')=='Positive') & (col('DRVR_ZIP').isNotNull()))                                .groupBy(col('DRVR_ZIP'))                                .agg(count("*").alias('count'))                                .orderBy(col("count").desc())                                .limit(5).select(col('DRVR_ZIP'))
            analytics6_final_df.show(truncate=False)
            out=wDFrame.Write_Dataframe(analytics6_final_df,'analytics6_output_path','csv',getpath.get_path_from_config('output_path','analytics6_output_path'))
            out.write()
        except Exception as e:    
            print(e) 
            


# In[ ]:




