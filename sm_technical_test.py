#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from sqlalchemy import create_engine
from postgress_settings import settings


# In[15]:


rows =  []
insteres_keys =['RecordTitle', 'Reference']
for i in range(1, 101):
    api_url = 'https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/gene/'+str(i)+'/JSON'
    response = requests.get(api_url)
    if response.status_code == 200:
        json_response = response.json()['Record']
        new_dict = { key: json_response[key] if key != 'Reference' else len(json_response[key]) for key in insteres_keys }
        rows.append(new_dict)


# In[18]:


df = pd.DataFrame(rows)
df.rename(columns = {'RecordTitle':'gene', 'Reference':'references_number'}, inplace = True)
df.sort_values('references_number', ascending = False, inplace=True)


# In[ ]:


user = settings['pguser']
passwd = settings['pgpasswd']
host = settings['pghost']
port = settings['pgport']
db = settings['pgdb']
url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
engine = create_engine(url)


# In[ ]:


df.to_sql('sm_test_table', engine)

