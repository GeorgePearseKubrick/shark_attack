#!/usr/bin/env python
# coding: utf-8

# In[10]:


import csv 
from datetime import datetime, timedelta
import pyodbc


# In[11]:


conn = pyodbc.connect('DSN=kubrickSQl;UID=de14;PWD=password')
cur = conn.cursor()


# In[12]:


sharkfile = r'C:\data\GSAF5.csv'


# In[13]:


attack_dates = []
case = []
activity = []
age = []
gender = []
country = []
isfatal = []
with open(sharkfile) as f: 
    reader = csv.DictReader(f)
    for row in reader: 
        case.append(row['Case Number'])
        attack_dates.append(row['Date'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])


# In[14]:


data = zip(attack_dates, case, country, activity, age, gender, isfatal)


# In[15]:


cur.execute('truncate table george.shark')


# In[16]:


q = 'insert into george.shark(attack_date, case_number, country, activity, age, gender, isfatal) values (?,?,?,?,?,?,?)'


# In[17]:


for d in data: 
    try:
        cur.execute(q, d)
        conn.commit()
        print('worked')
    except:
        conn.rollback()
        print('failed')


# In[ ]:




