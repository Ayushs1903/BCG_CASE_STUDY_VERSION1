#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *


# In[14]:


spark = SparkSession.builder          .appName("BCG_Car_Crash_CaseStudy")          .getOrCreate()


# # Import all solutions

# In[15]:


import Get_Path as getpath
import DataFrame_Create as DFrame
import Analytics_1_solution as analy1
import Analytics_2_solution as analy2
import Analytics_3_solution as analy3
import Analytics_4_solution as analy4
import Analytics_5_solution as analy5
import Analytics_6_solution as analy6
import Analytics_7_solution as analy7
import Analytics_8_solution as analy8


# # Create function to start all executions

# In[16]:


def main():
    '''This function is used to execut all solutions and print result'''
    analytics_1= analy1.Analytics_1_Solution()
    analytics_2= analy2.Analytics_2_Solution()
    analytics_3= analy3.Analytics_3_Solution()
    analytics_4= analy4.Analytics_4_Solution()
    analytics_5= analy5.Analytics_5_Solution()
    analytics_6= analy6.Analytics_6_Solution()
    analytics_7= analy7.Analytics_7_Solution()
    analytics_8= analy8.Analytics_8_Solution()
    print('Result of Analytics1:')
    analytics_1.analytics1()
    print('Result of Analytics2:')
    analytics_2.analytics2()
    print('Result of Analytics3:')
    analytics_3.analytics3()
    print('Result of Analytics4:')
    analytics_4.analytics4()
    print('Result of Analytics5:')
    analytics_5.analytics5()
    print('Result of Analytics6:')
    analytics_6.analytics6()
    print('Result of Analytics7:')
    analytics_7.analytics7()
    print('Result of Analytics8:')
    analytics_8.analytics8()


# In[17]:


if __name__=='__main__':
    main()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




