#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *


# In[ ]:


config_file_path='C:\\Users\\AYSINGH\\Downloads\\config.ini'


# In[ ]:


from configparser import ConfigParser
config=ConfigParser()
config.read(config_file_path)
#print(config.sections())


# # Create function to return path from Config File 

# In[ ]:


def get_path_from_config(section,subsection):
    '''This function returns path from config file'''
    return config[section][subsection]


# In[ ]:





# In[ ]:




