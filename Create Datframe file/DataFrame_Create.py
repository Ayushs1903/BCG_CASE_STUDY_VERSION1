#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *


# In[2]:


spark = SparkSession.builder          .appName("BCG_Car_Crash_CaseStudy")          .getOrCreate()


# # Class for creating dataframe

# In[5]:


class Dataframe:
    '''This class takes path as
    an input and creates an object whose df attribute is a dataframe'''
    
    def __init__(self,path):
        '''This function initializes variable and creates dataframe'''
        self.path=path
        
        self.df=self.createdf()
        self.distinct_records()
        
        
    def createdf(self):
        '''This function returns a dataframe from reading file from path'''
        try:
            if self.path is None:
                return
            return spark.read.format("csv").option("header","true").option("mode","permissive").option("inferSchema" , "true").load(self.path)
        
        except Exception as e:
            print(e)
    
    def distinct_records(self):
        '''This function takes distinct records of any dataframe'''
        self.df=self.df.distinct()
        


# In[ ]:




