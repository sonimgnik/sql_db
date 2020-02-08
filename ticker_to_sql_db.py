#!/usr/bin/env python
# coding: utf-8

# In[11]:


import numpy as np
import pandas as pd
import pandas_datareader as web
from datetime import date


# In[19]:


today = date.today()
data = web.DataReader('SPY', data_source = 'yahoo', start = '1/1/2000', end = today)
data.info()


# In[20]:


#data = data.reset_index()
#data.head()


# In[21]:


import sqlite3 as sq
data = data
sql_data = r'C:\Users\Nick\Desktop\ticker.db' #- Creates DB names SQLite


# In[22]:


conn = sq.connect('ticker.db')
c = conn.cursor()


# In[23]:


#cur.execute('DROP TABLE IF EXISTS ticker ') #DO NOT RUN.  THIS WILL DELETE EXISTING DATA
c.execute('CREATE TABLE SPY (Date, High, Low, Open, Close, Volume, Adj Close)')
conn.commit()


# In[24]:


data.to_sql('SPY', conn, if_exists='replace', index = True)


# In[25]:


c.execute('''SELECT * FROM SPY''')

#for row in c.fetchall():
#    print (row)

pd.read_sql('select * from SPY limit 5;', conn)


# In[26]:


c.execute('''SELECT * FROM SPY''')
df = pd.DataFrame(c.fetchall(), columns= ['Date','High', 'Low', 'Open', 'Close', 'Volume', 'Adj Close'])    
df


# In[27]:


conn.commit()
conn.close()


# In[33]:


con = sq.connect('ticker.db')
 
def sql_fetch(con):
 
    cursorObj = con.cursor()
 
    cursorObj.execute('SELECT name from sqlite_master where type= "table"')
 
    print(cursorObj.fetchall())
 
sql_fetch(con)


# In[34]:


con = sq.connect('ticker.db')
conn.close()


# In[ ]:




