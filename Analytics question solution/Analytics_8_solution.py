#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *


# In[ ]:





# In[ ]:



spark = SparkSession.builder          .appName("BCG_Car_Crash_CaseStudy")          .getOrCreate()


# In[ ]:





# In[ ]:


import Get_Path as getpath
import DataFrame_Create as DFrame
import Write_Dataframe as wDFrame


# In[ ]:





# # Create necessary dataframe

# In[ ]:





# In[ ]:


units=DFrame.Dataframe(getpath.get_path_from_config('input_path','units_input_path'))
charges=DFrame.Dataframe(getpath.get_path_from_config('input_path','charges_input_path'))


# In[ ]:





#  # Create class for analytics 8 solution

# In[ ]:





# In[ ]:




class Analytics_8_Solution:
    '''This class gives final result for analytics8'''
    
    def analytics8(self):
        '''8.	Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
         '''
        try:
            top_25_offense=units.df.groupBy(col('VEH_LIC_STATE_ID'))                                    .agg(countDistinct("CRASH_ID").alias("total_offences"))                                    .orderBy(col("total_offences").desc())                                    .limit(25)                                    .select("VEH_LIC_STATE_ID")                                    .collect()
            top_25_offense_list=[i['VEH_LIC_STATE_ID'] for i in top_25_offense ]
            
            
            top_10_colour= units.df.filter(col('VEH_COLOR_ID')!='NA')                                    .groupBy(col('VEH_COLOR_ID'))                                    .agg(count(col("CRASH_ID")).alias("total_colors_being_used"))                                    .orderBy(col("total_colors_being_used").desc())                                    .limit(10)                                    .select("VEH_COLOR_ID")                                    .collect()
            
            top_10_colour_list=[i['VEH_COLOR_ID'] for i in top_10_colour ]
            
            charges_with_speed=charges.df.where("lower(Charge) like '%speed%'").select("CRASH_ID","CHARGE")
            
            Units_with_filtered_conditions=units.df                                                .filter(col("VEH_COLOR_ID").isin(*top_10_colour_list) & col("VEH_LIC_STATE_ID").isin(*top_25_offense_list))                                                .select("CRASH_ID","VEH_MAKE_ID","VEH_COLOR_ID","VEH_LIC_STATE_ID")
            
            
            U=Units_with_filtered_conditions.alias("U")
            C=charges_with_speed.alias("C")
            analytics8_final_df=U.join(C,[U.CRASH_ID==C.CRASH_ID],"INNER")                                    .groupBy("VEH_MAKE_ID")                                    .agg(count("*").alias("Count_for_each_Veh_Makers"))                                    .orderBy(col("Count_for_each_Veh_Makers").desc())                                    .limit(5)
            
            analytics8_final_df.show(truncate=False)
            
            out=wDFrame.Write_Dataframe(analytics8_final_df,'analytics8_output_path','csv',getpath.get_path_from_config('output_path','analytics8_output_path'))
            out.write()
            
        except Exception as e:
            print(e)
            


# In[ ]:





# In[ ]:





# In[ ]:




