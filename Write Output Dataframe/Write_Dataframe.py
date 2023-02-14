#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *


# In[2]:


spark = SparkSession.builder          .appName("BCG_Car_Crash_CaseStudy")          .getOrCreate()


# In[4]:


config_file_path='C:\\Users\\AYSINGH\\Downloads\\config.ini'


# In[5]:


from configparser import ConfigParser
config=ConfigParser()
config.read(config_file_path)
print(config.sections())


# # Class to write result to output

# In[6]:


class Write_Dataframe:
    '''This class is used to write result to output path'''
    def __init__(self,df,func,format_of_out):
        '''This function is used to initialize parameters'''
        self.df=df
        self.func=func
        self.format=format_of_out
        self.path=config['output_path'][func]
        
    def write(self):
        try:
            path= self.path
            #print(path)
            #self.df.write.format(self.format).mode('overwrite').save(path)
            print('Result of '+self.func.split('_')[0]+' successfully written at path '+path)
        except Exception as e:
            print(e)
    


# In[ ]:




