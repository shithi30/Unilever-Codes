#!/usr/bin/env python
# coding: utf-8

# In[1]:


## import
from selenium import webdriver
from bs4 import BeautifulSoup
import requests
from PIL import Image
import os
import glob
from random import shuffle
import re
import pandas as pd
import duckdb
from googleapiclient.discovery import build
from google.oauth2 import service_account
import win32com.client
import time


# In[2]:


## scrape

# preferences
start_time = time.time()
options = webdriver.ChromeOptions()
options.add_argument("ignore-certificate-errors")

## headless
# options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
# options.add_argument("headless")

# open window
driver = webdriver.Chrome(options = options)
driver.maximize_window()


# In[3]:


# ## OHSOGO

# # url
# osgo_url = 'https://ohsogo.com/'
# driver.get(osgo_url)

# # accumulator
# img_links = []

# # soup
# soup_init = BeautifulSoup(driver.page_source, "html.parser")

# # slider
# try: soup = soup_init.find("div", attrs = {"class": "flickity-viewport"}).find_all("img")
# except: soup = []
# for s in soup: img_links.append("http:" + s["src"])

# # hero
# try: soup = soup_init.find("div", attrs = {"class": "icartShopifyCartContent"}).find_all("img")
# except: soup = []
# for s in soup: 
#     img_link = "http:" + s["src"]
#     if 'anner' in img_link: img_links.append(img_link)

# # # recommends
# # soup = soup_init.find("section", attrs = {"id": "shopify-section-template--22348999721253__gallery"}).find_all("img")
# # for s in soup: img_links.append("http:" + s["src"])

# # clear
# len_img = len(img_links)
# path = "C:/Users/shith/OneDrive/Banners by Account/OHSOGO/"
# files = glob.glob(path + "*")
# for f in files: ret = os.remove(f) if len_img > 0 else None

# # save
# for i in range(0, len_img):
#     img_data = Image.open(requests.get(img_links[i], stream = True, verify = True).raw).convert("RGB")
#     img_data.save(path + "osgo_" + str(i+1) + ".jpg", "JPEG")

# # record
# osgo_df = pd.DataFrame()
# osgo_df['banner_source'] = img_links
# osgo_df['platform'] = 'OHSOGO'
# osgo_df['platform_link'] = osgo_url


# In[4]:


## Pandamart

# url
urls = [
    'https://www.foodpanda.com.bd/darkstore/w2lx/pandamart-gulshan-w2lx',
    'https://www.foodpanda.com.bd/darkstore/h5rj/pandamart-bashundhara',
    'https://www.foodpanda.com.bd/darkstore/ta7z/pandamart-dhanmondi',
    'https://www.foodpanda.com.bd/darkstore/n7ph/pandamart-uttara',
    'https://www.foodpanda.com.bd/darkstore/v1ts/pandamart-mogbazar',
    'https://www.foodpanda.com.bd/darkstore/q4hz/pandamart-sylhet-02',
    'https://www.foodpanda.com.bd/darkstore/a2er/pandamart-khulna',
    'https://www.foodpanda.com.bd/darkstore/w2nv/pandamart-chittagong-1'
]
shuffle(urls)
driver.get(urls[0])

# soup
soup_init = BeautifulSoup(driver.page_source, "html.parser")
soup = soup_init.find_all("img", attrs={"data-testid": "campaign-banners-swiper-groceries-img"})
len_soup = len(soup)

# clear
path = "C:/Users/shith/OneDrive/Banners by Account/Pandamart/"
files = glob.glob(path + "*")
for f in files: ret = os.remove(f) if len_soup > 0 else None

# save
for i in range(0, len_soup): 
    img_link = soup[i]["src"]  
    img_data = Image.open(requests.get(img_link, stream = True, verify = True).raw).convert("RGB")
    img_data.save(path + "pmrt_" + str(i+1) + ".jpg", "JPEG")

# record
pmrt_df = pd.DataFrame()
pmrt_df['banner_source'] = [soup[i]["src"] for i in range(0, len_soup)]
pmrt_df['platform'] = 'Pandamart'
pmrt_df['platform_link'] = urls[0]


# In[5]:


## Daraz

# url
daraz_url = 'https://www.daraz.com.bd'
driver.get(daraz_url)

# soup
soup_init = BeautifulSoup(driver.page_source, "html.parser")
soup = soup_init.find_all("img", attrs={"class": "main-img"})
len_soup = len(soup)

# clear
path = "C:/Users/shith/OneDrive/Banners by Account/Daraz/"
files = glob.glob(path + "*")
for f in files: ret = os.remove(f) if len_soup > 0 else None

# save
img_links = []
for i in range(0, len_soup): 
    try: img_links.append("http:" + soup[i]["data-ks-lazyload"])
    except: img_links.append("http:" + soup[i]["src"])
    img_data = Image.open(requests.get(img_links[i], stream = True, verify = True).raw).convert("RGB")
    img_data.save(path + "darz_" + str(i+1) + ".jpg", "JPEG")

# record
daaz_df = pd.DataFrame()
daaz_df['banner_source'] = img_links
daaz_df['platform'] = 'Daraz'
daaz_df['platform_link'] = daraz_url


# In[6]:


## Shajgoj

# url
shaj_url = 'https://shop.shajgoj.com/'
driver.get(shaj_url)

# accumulator
img_links = []

# horizontal
soup_init = BeautifulSoup(driver.page_source, "html.parser")
soup = soup_init.find_all("div", attrs={"class": "wpb_single_image wpb_content_element vc_align_center"})
for s in soup: img_links.append(s.find("img")["src"])
    
# carousel
regex = re.compile("slider-3801 slide.*")
soup = soup_init.find_all("img", attrs={"class": regex})
for s in soup: img_links.append(s["src"])

# grid
soup = soup_init.find_all("div", attrs={"class": "vc_row"})
len_soup = len(soup)
for i in range(0, len_soup):
    if soup[i].get_text() not in ('TOP BRANDS & OFFERS', 'DEALS YOU CANNOT MISS') or 'hide-for-now' in soup[i+1]["class"]: continue
    banners = soup[i+1].find_all("img")
    for bnr in banners: img_links.append(bnr["src"])

# clear
len_img = len(img_links)
path = "C:/Users/shith/OneDrive/Banners by Account/Shajgoj/"
files = glob.glob(path + "*")
for f in files: ret = os.remove(f) if len_img > 0 else None

# save
for i in range(0, len_img):
    img_data = Image.open(requests.get(img_links[i], stream = True, verify = True).raw).convert("RGB")
    img_data.save(path + "shaj_" + str(i+1) + ".jpg", "JPEG")

# record
shaj_df = pd.DataFrame()
shaj_df['banner_source'] = img_links
shaj_df['platform'] = 'Shajgoj'
shaj_df['platform_link'] = shaj_url


# In[7]:


## services

# credentials
SERVICE_ACCOUNT_FILE = "read-write-to-gsheet-apis-1-04f16c652b1e.json"
SAMPLE_SPREADSHEET_ID = "1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()


# In[9]:


## ETL

# extract
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Banners!B1:F').execute().get('values', [])
prev_df = pd.DataFrame(values[1:] , columns = values[0])

# transform
pres_df = duckdb.query('''select * from pmrt_df union all select * from daaz_df union all select * from shaj_df /*union all select * from osgo_df*/''').df()
qry = '''
-- old
select banner_source, platform, platform_link, 0 if_new, report_time from prev_df union
-- new
select banner_source, platform, platform_link, 1 if_new, strftime(now(), '%d-%b-%y, %I:%M %p') report_time
from pres_df
where banner_source not in(select banner_source from prev_df)
'''
pres_df = duckdb.query(qry).df()

# load
sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Banners!B1:F').execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Banners!B1', valueInputOption='USER_ENTERED', body={'values': [pres_df.columns.values.tolist()] + pres_df.fillna('').values.tolist()}).execute()
driver.close()


# In[10]:


## new

# new banners
new_br_df = duckdb.query('''select * from pres_df where if_new=1''').df()
new_pltfm = new_br_df['platform'].tolist()
new_links = new_br_df['banner_source'].tolist()
len_links = len(new_links)

# clear
path = "C:/Users/shith/OneDrive/Banners by Account/New/"
files = glob.glob(path + "*")
for f in files: ret = os.remove(f) if len_links > 0 else None

# save
for i in range(0, len_links): 
    img_link = new_links[i]
    img_data = Image.open(requests.get(img_link, stream = True, verify = True).raw).convert("RGB")
    img_data.save(path + "new_" + str(i+1) + "_" + new_pltfm[i] + ".jpg", "JPEG")


# In[11]:


## email

# object
ol = win32com.client.Dispatch("outlook.application")
newmail = ol.CreateItem(0x0)

# subject
newmail.Subject = "Banners by Accounts (" + str(len_links) + "*)" if len_links > 0 else "Banners by Accounts"

# expressions, ref: https://www.w3schools.com/charsets/ref_emoji.asp
emos = [128194, 128680, 128203, 128345] # folders, alarm, list, clock

# body
newmail.HTMLbody = '''
Dear concern,<br>
<br>
Please click to find:<br>
''' + "&#" + str(emos[0]) + ''' Account-wise banners, <a href="https://1drv.ms/f/s!AnD8IACnC-3ylFt5nUMw7SLpJaWK?e=GZKVwM"> here.</a><br>
''' + "&#" + str(emos[1]) + ''' New banners <b>(''' + str(len_links) + ''')</b>, <a href="https://1drv.ms/f/s!AnD8IACnC-3ylyTfOza6HEWDU8_g?e=hlSXNF"> here</a> (attached).<br>
''' + "&#" + str(emos[2]) + '''  Full, historical list of all banners, <a href="https://docs.google.com/spreadsheets/d/1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0/edit#gid=797369123"> here.</a><br>
''' + "&#" + str(emos[3]) + ''' Elapsed time to scrape (sec): ''' + str(round(time.time() - start_time)) + '''<br>
<br>
Thanks,<br>
Shithi Maitra<br>
Asst. Manager, CSE<br>
Unilever BD Ltd.<br>
'''

# attach
path = "C:/Users/shith/OneDrive/Banners by Account/New/"
files = glob.glob(path + "*")
for f in files: ret = newmail.Attachments.Add(f) if len_links > 0 else None

# send
newmail.To = "avra.barua@unilever.com; safa-e.nafee@unilever.com; rafid-al.mahmood@unilever.com"
newmail.BCC = "shithi30@outlook.com"
if len_links > 0: newmail.Send()


# In[ ]:




